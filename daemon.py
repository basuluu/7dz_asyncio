from aiohttp import web
import aiohttp
import asyncio
import aiofiles
import os
import yaml

class Daemon():
    def __init__(self, config):
        with open(config, 'r') as cfg:
            try:
                data = yaml.load(cfg)
                self.port = data['port']
                if data['directory']:
                    self.path = data['directory'] + '/'
                    self.path_read = self.path
                else:
                    self.path = '.'
                    self.path_read = ''
                self.node_list = data['node_list']
                self.save_flag = data['save']
            except yaml.YAMLError as exc:
                print(exc)
        self._daemon = web.Application()
        self._daemon.add_routes([
            web.get('/{name}', self.handle_get),
            web.delete('/{name}', self.handle_delete),
        ])
        web.run_app(self._daemon, port=self.port)

    async def check(self, request):
        name = request.match_info.get('name')
        text = ''
        if name in os.listdir(self.path):
            async with aiofiles.open(self.path_read + name) as f:
                text = await f.read()
        return text, name

    async def handle_get(self, request):
        text, name = await self.check(request)
        if not text:
            async with aiohttp.ClientSession() as session:
                futures = []
                for node in self.node_list:
                    futures.append(session.delete(f"http://{node['host']}:{node['port']}/{name}"))
                    for future in asyncio.as_completed(futures):
                        resp = await future
                        text = await resp.text()
                        if text:
                            break
        if text:
            if self.save_flag:
                async with aiofiles.open(self.path_read + name, 'w') as f:
                    await f.write(text)
            return web.Response(text=text)
        return web.Response(status=404)

    async def handle_delete(self, request):
        text, _ = await self.check(request)
        return web.Response(text=text)


Daemon("config.yml")
