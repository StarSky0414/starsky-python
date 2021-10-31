import _thread
import asyncio
import threading
import time

import aiohttp
from tqdm import tqdm
import os

from pool.mysqlhelper import MySqLHelper
from proto.outFile import down_file_pb2_grpc, down_file_pb2
from pool.RedisPool import makeredisconn


class bigfile_download:

    def __init__(self, session, url, tmp_path='./down_cache', proxy=None, file_fragment_size=1024 * 1024 * 2):
        self.url = url
        self.session = session
        self.proxy = proxy
        self.filename = url.split('/')[-1].split('?')[0]
        self.mtd_list = []
        self.tmp_path = tmp_path
        self.file_fragment_size = file_fragment_size
        self.__mkdir(tmp_path)

    def __mkdir(self, path):
        isExists = os.path.exists(path)
        if not isExists:
            os.makedirs(path)
            print(path + ' 创建成功')
            return True
        else:
            print(path + ' 目录已存在')
            return False

    async def fetch(self, url, method='get', headers=None, retryCount=3):
        r = None
        curr_url = url
        for i in range(1, retryCount):
            if method == 'head':
                r = await self.session.head(curr_url, proxy=self.proxy, headers=headers)
            else:
                r = await self.session.get(curr_url, proxy=self.proxy, headers=headers)
            if r.status in (301, 302):
                curr_url = r.headers['Location']
                r.close()
            else:
                return r
        return r

    async def get_content_length_from_net(self):
        try:
            r = await self.fetch(self.url, 'head')
            self.filesize = int(r.headers['Content-Length'])
        finally:
            if r != None:
                r.close()

        print("filesize = {0}".format(self.filesize))

    async def calculation_fragment(self):
        filesize = self.filesize
        start = 0
        end = -1
        step = self.file_fragment_size

        file_fragment_n = 1

        while end < filesize - 1:
            start = end + 1
            end = start + step - 1
            total_size = end - start + 1

            if end > filesize:
                total_size = end - start
                end = filesize

            headers = {'Range': 'bytes={0}-{1}'.format(start, end)}
            self.mtd_list.append((file_fragment_n, start, end, headers, total_size))

            file_fragment_n = file_fragment_n + 1

        print("file_fragment_num = {0}".format(len(self.mtd_list)))
        redisconn.hset(self.filename,"num",len(self.mtd_list))
        redisconn.hset(self.filename, "num_finish", 0)
        db = MySqLHelper()
        db.insertone("INSERT into t_down_file (file_name,file_num,file_url,file_create_time,file_size)  VALUES (%s,%s,%s,NOW(),%s)  ",(self.filename,len(self.mtd_list),self.url,StrOfSize(self.filesize)))


    async def fragment_down(self, mtd):
        target_filename = '{2}/{0}.{1}'.format(self.filename, mtd[0], self.tmp_path)
        total_size = mtd[4]
        if os.path.exists(target_filename):
            target_filename_size = os.path.getsize(target_filename)
            if total_size == target_filename_size:
                return
            else:
                os.remove(target_filename)

        print(mtd)
        pbar = tqdm(desc='task{0}'.format(mtd[0]), total=mtd[4], leave=False)
        r = None
        try:
            r = await self.fetch(self.url, headers=mtd[3])
            with open(target_filename, 'wb') as f:
                async for chunk, _ in r.content.iter_chunks():
                    f.write(chunk)
                    chunk_size = len(chunk)
                    pbar.update(chunk_size)
        finally:
            redisconn.hincrby(self.filename, "num_finish", 1)
            if r != None:
                r.close()

    async def fragment_down_all(self, taskPoolMaxNum):
        dltasks = set()

        for mtd in self.mtd_list:
            if len(dltasks) >= taskPoolMaxNum:
                dones, dltasks = await asyncio.wait(dltasks, return_when=asyncio.FIRST_COMPLETED)
            dltasks.add(asyncio.ensure_future(self.fragment_down(mtd)))
        dones, dltasks = await asyncio.wait(dltasks)

    async def fragment_merge(self, target_path=None):
        if target_path != None:
            target_filename = target_path + self.filename
        else:
            target_filename = self.filename

        with open(target_filename, 'wb') as newfile:
            for mtd in self.mtd_list:
                target_fragment_filename = '{1}/{0}.{2}'.format(self.filename, self.tmp_path, mtd[0])
                with open(target_fragment_filename, 'rb') as fragment_file:
                    newfile.write(fragment_file.read())
        redisconn.hset(self.filename,"file_path",target_filename)
        db = MySqLHelper()
        db.update("UPDATE t_down_file SET file_finsh_time = NOW() , file_path = %s , file_num_finsh = %s WHERE file_name=%s ",(target_path,len(self.mtd_list),self.filename))
        print('fragment_merge end!')

    async def fragment_down_check(self):
        for mtd in self.mtd_list:
            target_filename = '{2}/{0}.{1}'.format(self.filename, mtd[0], self.tmp_path)
            total_size = mtd[4]
            if os.path.exists(target_filename):
                target_filename_size = os.path.getsize(target_filename)
                if total_size == target_filename_size:
                    return True
                else:
                    print('fragment_down_check error1!')
                    return False
            else:
                print('fragment_down_check error2!')
                return False

        print('fragment_down_check ok!')
        return True


async def download_bigfile(url, proxy=None):
    file_fragment_size = 1024 * 1024 * 2
    task_num_max = 5
    tmp_path = './down_cache'
    target_path = '/root/vue/image/down_file/'

    async with aiohttp.ClientSession() as session:
        bd = bigfile_download(session, url, tmp_path=tmp_path, proxy=proxy, file_fragment_size=file_fragment_size)
        await bd.get_content_length_from_net()
        await bd.calculation_fragment()
        for i in range(1, 10):
            if await bd.fragment_down_check() == True:
                await bd.fragment_merge(target_path)
                break
            await bd.fragment_down_all(task_num_max)

#定义一个专门创建事件循环loop的函数，在另一个线程中启动它
def start_loop(loop,downUrl):

    asyncio.set_event_loop(loop)
    loop.run_until_complete(download_bigfile(downUrl))

def StrOfSize(size):
    '''
    auth: wangshengke@kedacom.com ；科达柯大侠
    递归实现，精确为最大单位值 + 小数点后三位
    '''
    def strofsize(integer, remainder, level):
        if integer >= 1024:
            remainder = integer % 1024
            integer //= 1024
            level += 1
            return strofsize(integer, remainder, level)
        else:
            return integer, remainder, level

    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    integer, remainder, level = strofsize(size, 0, 0)
    if level+1 > len(units):
        level = -1
    return ( '{}.{:>03d} {}'.format(integer, remainder, units[level]) )


#存储运行时候的数据
# def saveRunInfo():
redisconn = makeredisconn()


class DownFile(down_file_pb2_grpc.FileServicer):

    def downFile(self, request, context):
        print("Received request: %s" % request)
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            t = threading.Thread(target=start_loop, args=(loop, request.downUrl))  # 通过当前线程开启新的线程去启动事件循环
            t.start()
        except RuntimeError:
            return down_file_pb2.DownFileSynchronizationResultResponse(code=0, message="运行失败")


        return down_file_pb2.DownFileSynchronizationResultResponse(code=1,message="OK")