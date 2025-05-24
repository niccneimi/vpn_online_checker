import os
import time
import re
from datetime import datetime, timedelta
from collections import defaultdict
import socket
import asyncio
from typing import Union
from dotenv import load_dotenv
import asyncpg
from aiohttp import web

EMAIL_RE = re.compile(r'email:\s*([a-zA-Z0-9\-]+(?:@[a-zA-Z0-9\-.]+)?[a-zA-Z0-9])')
IP_RE = re.compile(r'\b(?:25[0-5]|2[0-4][0-9]|1?\d{1,2})\.(?:25[0-5]|2[0-4][0-9]|1?\d{1,2})\.(?:25[0-5]|2[0-4][0-9]|1?\d{1,2})\.(?:25[0-5]|2[0-4][0-9]|1?\d{1,2})\b')
hostname = socket.gethostname()
local_ip = socket.gethostbyname(hostname)

load_dotenv()

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: Union[asyncpg.Pool, None] = None

    async def create_pool(self):
        self._pool = await asyncpg.create_pool(self.dsn)

    async def close_pool(self):
        if self._pool:
            await self._pool.close()

    async def update_online_datas(self, ips, host, uuid):
        online_count = len(ips)
        ips_string = ':'.join(ips)
        sql = """UPDATE clients_as_keys SET online_count = $1, online_ips = $2 WHERE host = $3 AND uuid = $4"""
        async with self._pool.acquire() as conn:
            await conn.execute(sql, online_count, ips_string, host, uuid)
            
    async def get_user_ips(self, uuid: str) -> tuple[int, list[str]]:
        sql = """SELECT 
    SUM(online_count) AS total_online_count,
    STRING_AGG(online_ips, ':' ORDER BY host) AS all_online_ips
    FROM clients_as_keys
    WHERE uuid = $1;
"""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, uuid)
            if not row or not row['online_ips']:
                return 0, []
            return row['online_count'], row['online_ips'].split(':')

async def monitor_users(log_path, db: Database, timeout_min=0.5):
    user_data = defaultdict(lambda: defaultdict(lambda: datetime.min))
    current_pos = 0

    while True:
        print(local_ip)
        try:
            with open(log_path, 'r') as f:
                f.seek(current_pos)
                new_lines = f.readlines()
                current_pos = f.tell()

                for line in new_lines:
                    email_match = EMAIL_RE.search(line)
                    ip_match = IP_RE.search(line)
                    if not email_match or not ip_match:
                        continue

                    email = email_match.group(1)
                    ip = ip_match.group(0)

                    try:
                        time_str = ' '.join(line.split()[:2]).split('.')[0]
                        log_time = datetime.strptime(time_str, "%Y/%m/%d %H:%M:%S")
                    except Exception:
                        continue

                    if log_time > user_data[email][ip]:
                        user_data[email][ip] = log_time

                now = datetime.now()
                print(f"Online users ({now.strftime('%Y-%m-%d %H:%M:%S')}):\n")

                for email, ip_dict in user_data.items():
                    active_ips = [ip for ip, last_seen in ip_dict.items()
                                  if now - last_seen <= timedelta(minutes=timeout_min)]
                    if active_ips:
                        print(f"Email: {email}")
                        print("Active IPs:", ", ".join(active_ips))

                        await db.update_online_datas(active_ips, local_ip, email)
                    else:
                        await db.update_online_datas([], local_ip, email)

        except FileNotFoundError:
            print(f"Файл {log_path} не найден")
            break

        await asyncio.sleep(10)

async def auth_handler(request: web.Request) -> web.Response:
    db: Database = request.app['db']
    data = await request.json()
    
    email = data.get('email')
    ip = data.get('ip')
    
    if not email or not ip:
        return web.json_response({"allow": False}, status=400)
    
    try:
        online_count, online_ips = await db.get_user_ips(email)
        
        if online_count >= 3:
            allow = ip in online_ips
        else:
            allow = True
            
        return web.json_response({"allow": allow})
    
    except Exception as e:
        print(f"Auth error: {str(e)}")
        return web.json_response({"allow": False}, status=500)

async def run_server(db: Database):
    app = web.Application()
    app['db'] = db
    app.router.add_post('/auth', auth_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    print("Authorization server started on port 8080")

if __name__ == "__main__":
    dsn = f"postgresql://{os.getenv('DATABASE_USERNAME')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}/{os.getenv('DATABASE_NAME')}"
    db = Database(dsn)

    async def main():
        await db.create_pool()
        server_task = asyncio.create_task(run_server(db))
        monitor_task = asyncio.create_task(
            monitor_users("/var/log/xray/access.log", db, timeout_min=0.5)
        )
        try:
            await asyncio.gather(server_task, monitor_task)
        finally:
            await db.close_pool()

asyncio.run(main())
