import asyncio
import json
from aiohttp import web
from enum import IntEnum
import argparse
import sys

# 定义任务状态
class TaskStatus(IntEnum):
    UNPROCESS = 0
    DISTRIBUTE = 1
    COMPLETE = 2
    ERROR = 3
    ABANDON = 4

# 任务管理模块
class TaskManager:
    def __init__(self, tasks_json_path, samples_list_path):
        self.tasks_json_path = tasks_json_path
        self.samples_list_path = samples_list_path
        self.tasks = self.load_tasks(tasks_json_path)
        self.samples = self.load_samples(samples_list_path)
        self.status = {sample: {task['name']: TaskStatus.UNPROCESS for task in self.tasks['pipeline']} for sample in self.samples}
        self.locks = {sample: asyncio.Lock() for sample in self.samples}
        self.update_lock = asyncio.Lock()  # 用于更新操作的锁

    def load_tasks(self, path):
        with open(path) as f:
            return json.load(f)

    def load_samples(self, path):
        with open(path) as f:
            return [line.strip() for line in f.readlines()]

    async def update_tasks(self):
        async with self.update_lock:
            new_tasks = self.load_tasks(self.tasks_json_path)
            for task in new_tasks['pipeline']:
                if task['name'] not in [t['name'] for t in self.tasks['pipeline']]:
                    self.tasks['pipeline'].append(task)
                    for sample in self.samples:
                        self.status[sample][task['name']] = TaskStatus.UNPROCESS

    async def update_samples(self):
        async with self.update_lock:
            new_samples = self.load_samples(self.samples_list_path)
            for sample in new_samples:
                if sample not in self.samples:
                    self.samples.append(sample)
                    self.status[sample] = {task['name']: TaskStatus.UNPROCESS for task in self.tasks['pipeline']}
                    self.locks[sample] = asyncio.Lock()

    async def get_ready_sample(self, task_name):
        for sample in self.samples:
            async with self.locks[sample]:
                if self.is_sample_ready(sample, task_name):
                    self.status[sample][task_name] = TaskStatus.DISTRIBUTE
                    return sample
        return None

    def is_sample_ready(self, sample, task_name):
        task_info = next((task for task in self.tasks['pipeline'] if task['name'] == task_name), None)
        if task_info:
            if all(self.status[sample][upstream] == TaskStatus.COMPLETE for upstream in task_info['upstream']):
                return self.status[sample][task_name] == TaskStatus.UNPROCESS
        return False
    
    def get_task_status_csv(self, task_name=None):
        header = ['Sample'] + [task['name'] for task in self.tasks['pipeline']] if not task_name else ['Sample', task_name]
        lines = [header]
        for sample in self.samples:
            if task_name:
                line = [sample, str(self.status[sample].get(task_name, ''))]
            else:
                line = [sample] + [str(self.status[sample].get(task['name'], '')) for task in self.tasks['pipeline']]
            lines.append(line)
        return '\n'.join(['\t'.join(line) for line in lines])

# HTTP 请求处理，与路由设置对应
async def get_sample(request):
    task_name = request.match_info['task_name']
    task_manager = request.app['task_manager']
    sample = await task_manager.get_ready_sample(task_name)
    if sample:
        return web.Response(text = sample)
    else:
        return web.Response(status = 404, text = 'No ready samples found')

async def report_status(request):
    status = request.match_info['status']
    task_name = request.match_info['task_name']
    sample_name = request.match_info['sample_name']
    task_manager = request.app['task_manager']
    async with task_manager.locks[sample_name]:
        if sample_name in task_manager.status and task_name in task_manager.status[sample_name]:
            task_manager.status[sample_name][task_name] = TaskStatus[status.upper()]
            return web.Response(text = f"Updated {sample_name} for {task_name} to {status}")
        else:
            return web.Response(status=404, text = 'Sample or task not found')

async def update_tasks(request):
    await request.app['task_manager'].update_tasks()
    return web.Response(text = 'Tasks updated successfully')

async def update_samples(request):
    await request.app['task_manager'].update_samples()
    return web.Response(text = 'Samples updated successfully')

async def get_task_status(request):
    task_name = request.match_info.get('task_name')
    task_manager = request.app['task_manager']
    csv_data = task_manager.get_task_status_csv(task_name)
    return web.Response(text=csv_data, content_type='text/csv')

# 设置路由
def setup_routes(app):
    app.router.add_get('/get_sample/{task_name}', get_sample)
    app.router.add_get('/report/{status}/{task_name}/{sample_name}', report_status)
    app.router.add_get('/update_tasks', update_tasks)
    app.router.add_get('/update_samples', update_samples)
    app.router.add_get('/get_task_status/', get_task_status)
    app.router.add_get('/get_task_status/{task_name}', get_task_status)

# 解析命令行参数
def parse_args():
    parser = argparse.ArgumentParser(description = 'Async Web Server for Task and Sample Management')
    parser.add_argument('--tasks-json-dir', required = True, type = str, help = 'Path to the JSON file containing tasks')
    parser.add_argument('--samples-list-dir', required = True, type = str, help = 'Path to the TXT file containing samples list')
    parser.add_argument('--port', type = int, default = 10080, help = 'Port number to run the web server on')
    return parser.parse_args()

# 初始化应用
def init_app(argv):
    args = parse_args()
    app = web.Application()
    app['task_manager'] = TaskManager(args.tasks_json_dir, args.samples_list_dir)
    setup_routes(app)
    return app, args.port

if __name__ == '__main__':
    app, port = init_app(sys.argv[1:])
    web.run_app(app, port = port)
