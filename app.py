#!/usr/bin/env python3
"""
Unlimited AI API 中转服务
将 Unlimited AI 的 API 转换为 OpenAI 兼容格式
"""

from flask import Flask, request, jsonify, Response
import requests
import json
import os

app = Flask(__name__)

# 配置
API_KEY = os.environ.get('UNLIMITED_API_KEY', 'ua_6SVplC1_8oyQAAcs7OZSjknTqWi6-mQP')
BASE_URL = 'https://unlimited.surf'

@app.route('/health')
def health():
    return jsonify({'ok': True, 'service': 'Unlimited AI Proxy'})

@app.route('/v1/models')
def list_models():
    """返回 OpenAI 兼容的模型列表"""
    try:
        response = requests.get(f'{BASE_URL}/api/models', 
                              headers={'Authorization': f'Bearer {API_KEY}'},
                              timeout=10)
        data = response.json()
        
        # 转换为 OpenAI 格式
        models = []
        for model in data.get('data', []):
            models.append({
                'id': model['id'],
                'object': 'model',
                'created': 1700000000,
                'owned_by': model.get('provider', 'unlimited'),
                'permission': [],
                'root': model['id'],
                'parent': None
            })
        
        return jsonify({
            'object': 'list',
            'data': models
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/v1/chat/completions', methods=['POST'])
def chat_completions():
    """OpenAI 兼容的聊天完成接口"""
    try:
        data = request.json
        model = data.get('model', 'gateway-claude-opus-4-7')
        messages = data.get('messages', [])
        stream = data.get('stream', False)
        
        # 提取最后一条用户消息
        prompt = ''
        for msg in reversed(messages):
            if msg.get('role') == 'user':
                prompt = msg.get('content', '')
                break
        
        if not prompt:
            return jsonify({'error': 'No user message found'}), 400
        
        # 调用 Unlimited AI API
        payload = {
            'model': model,
            'prompt': prompt,
            'stream': True  # 始终使用stream模式
        }
        
        response = requests.post(f'{BASE_URL}/api/chat',
                               headers={'Authorization': f'Bearer {API_KEY}'},
                               json=payload,
                               stream=True,
                               timeout=60)
        
        if response.status_code != 200:
            return jsonify({'error': f'Upstream error: {response.status_code}'}), 502
        
        # 解析SSE响应
        content = ''
        for line in response.iter_lines():
            if line:
                line = line.decode('utf-8')
                if line.startswith('data: '):
                    data_str = line[6:]
                    try:
                        data = json.loads(data_str)
                        if 'delta' in data:
                            content += data['delta']
                        elif 'error' in data:
                            return jsonify({'error': data['error']}), 502
                    except json.JSONDecodeError:
                        pass
        
        # 返回OpenAI格式响应
        return jsonify({
            'id': 'chatcmpl-001',
            'object': 'chat.completion',
            'created': 1700000000,
            'model': model,
            'choices': [{
                'index': 0,
                'message': {
                    'role': 'assistant',
                    'content': content
                },
                'finish_reason': 'stop'
            }],
            'usage': {
                'prompt_tokens': len(prompt.split()),
                'completion_tokens': len(content.split()),
                'total_tokens': len(prompt.split()) + len(content.split())
            }
        })
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Request timeout'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/v1/chat/completions/stream', methods=['POST'])
def chat_completions_stream():
    """OpenAI 兼容的流式聊天完成接口"""
    try:
        data = request.json
        model = data.get('model', 'gateway-claude-opus-4-7')
        messages = data.get('messages', [])
        
        # 提取最后一条用户消息
        prompt = ''
        for msg in reversed(messages):
            if msg.get('role') == 'user':
                prompt = msg.get('content', '')
                break
        
        if not prompt:
            return jsonify({'error': 'No user message found'}), 400
        
        # 调用 Unlimited AI API
        payload = {
            'model': model,
            'prompt': prompt,
            'stream': True
        }
        
        def generate():
            response = requests.post(f'{BASE_URL}/api/chat',
                                   headers={'Authorization': f'Bearer {API_KEY}'},
                                   json=payload,
                                   stream=True,
                                   timeout=60)
            
            for line in response.iter_lines():
                if line:
                    line = line.decode('utf-8')
                    if line.startswith('data: '):
                        data_str = line[6:]
                        try:
                            data = json.loads(data_str)
                            if 'delta' in data:
                                chunk = {
                                    'id': 'chatcmpl-001',
                                    'object': 'chat.completion.chunk',
                                    'created': 1700000000,
                                    'model': model,
                                    'choices': [{
                                        'index': 0,
                                        'delta': {'content': data['delta']},
                                        'finish_reason': None
                                    }]
                                }
                                yield f"data: {json.dumps(chunk)}\n\n"
                            elif 'finish' in data:
                                chunk = {
                                    'id': 'chatcmpl-001',
                                    'object': 'chat.completion.chunk',
                                    'created': 1700000000,
                                    'model': model,
                                    'choices': [{
                                        'index': 0,
                                        'delta': {},
                                        'finish_reason': 'stop'
                                    }]
                                }
                                yield f"data: {json.dumps(chunk)}\n\n"
                                yield "data: [DONE]\n\n"
                            elif 'error' in data:
                                yield f"data: {json.dumps({'error': data['error']})}\n\n"
                        except json.JSONDecodeError:
                            pass
        
        return Response(generate(), mimetype='text/event-stream')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
