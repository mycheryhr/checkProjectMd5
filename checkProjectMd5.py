# /usr/bin/env python
# coding: utf-8


import os
import sys
import hashlib
import logging
import paramiko
import ConfigParser
from Queue import Queue
from threading import Thread


def cur_file_dir():
    """
    获取脚本绝对路径
    :return: string
    """
    path = sys.path[0]
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)


def get_file_md5_via_ssh(info):
    """
    通过ssh连接远程机器执行扫瞄项目目录所有文件(排除自定义的忽略文件和目录)，并获取所有文件的MD5值
    :param info: tuple or list : (项目名称, 主机, 项目目录)
    :return: 项目名str, 主机str, ssh输出str
    """
    logging.debug('ssh start')
    project, host, path = info
    if ssh_key:
        # 使用key文件进行连接
        logging.info('use ssh key')
        key = paramiko.RSAKey.from_private_key_file(ssh_key)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_system_host_keys()
        ssh.connect(host, ssh_port, ssh_user, pkey=key)
    else:
        # 使用密码连接
        logging.info('use password')
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, ssh_port, ssh_user, ssh_password)

    # shell命令：查找项目目录下所有文件，忽略自定义的目录和文件，再计算每个文件的md5，最后按文件全路径名排序
    command = 'find %s %s -prune -o -type f -print | xargs -i -P10 md5sum {}' % (path, ignore_format)

    stdin, stdout, stderr = ssh.exec_command(command)
    sshstdout = stdout.read()
    if sshstdout:
        return project, host, sshstdout
    else:
        sshstderr = stderr.read()
        #logging.error('ssh execute command error : {}'.format(sshstderr))
        return project, host, sshstderr


if __name__ == "__main__":
    script_path = cur_file_dir()

    # level定义日志输出的最低等级，ERROR表示只输出错误信息，DEBUG可以输出更全信息。filename为日志文件位置，默认同此脚本在一个目录
    #logging.basicConfig(level=logging.ERROR,
    logging.basicConfig(level=logging.ERROR,
                        format='%(asctime)s %(filename)s %(levelname)s %(message)s',
                        filename=os.path.join(script_path, 'my.log'),
                        filemode='a')
    try:
        # 配置读取并始初化配置
        f = ConfigParser.ConfigParser()
        f.optionxform = str
        f.read(os.path.join(script_path, 'project.conf'))
        logging.info('read config file')

        ignore_dir = f.get('ignore', 'dir').split() if f.has_option('ignore', 'dir') else []
        ignore_file = f.get('ignore', 'file').split() if f.has_option('ignore', 'file') else []
        ig_dir_format = [' -path "*/{}" '.format(i) for i in ignore_dir]
        ig_file_format = map(lambda s: ' -name "*{}" '.format(s) if s.startswith('.') else ' -name "*.{}" '.format(s), ignore_file)
        all_ig = ig_dir_format[:]
        all_ig.extend(ig_file_format)
        ignore_format = '\( {} \)'.format(' -o '.join(all_ig))
        projects_list = [i for i in f.sections() if i != 'ssh' and i != 'ignore' and i != 'thread']
        ssh_user = f.get('ssh', 'user') if f.has_option('ssh', 'user') else 'root'
        ssh_port = f.getint('ssh', 'port') if f.has_option('ssh', 'port') else 22
        ssh_key = f.get('ssh', 'key') if f.has_option('ssh', 'key') else ''
        ssh_password = f.get('ssh', 'password') if f.has_option('ssh', 'password') else ''

        if not ssh_key and not ssh_password:
            print 'Need Pubkey Or Password To Host'
            sys.exit(2)

        if not os.path.isfile(ssh_key):
            print 'Invalid Pubkey File'
            sys.exit(2)

        if not projects_list:
            print 'Miss Project'
            sys.exit(2)
    except:
        print 'Read Config File Error'
        sys.exit(2)

    # 使用队列，长度不限
    q = Queue(maxsize=0)

    for p in projects_list:
        if f.has_option(p, 'host') and f.has_option(p, 'path'):
            project_path = f.get(p, 'path')
            project_host = f.get(p, 'host').split()
            if project_path and project_host:
                for h in project_host:
                    # 项目名, 主机, 项目路径, 三者以元组形式推入队列
                    q.put((p, h, project_path))
            else:
                print 'Invalid Project Host or Path'
                sys.exit(2)

        else:
            print 'Miss Project\'s Host Or Path'
            sys.exit(2)

    logging.info('queue size %s' % q.qsize())

    def consumer(q, result):
        # 队列消费函数
        while not q.empty():
            work = q.get()
            logging.debug('get work')
            try:
                project, host, output = get_file_md5_via_ssh(work)
                logging.info('{} {} {}'.format(project, host, len(output)))
                output = '\n'.join(sorted(output.split('\n')))

            except Exception, e:
                print e
                q.task_done()
                sys.exit(2)

            result[project][host] = {}

            # 为ssh输出的所有文件MD5的全量字符计算MD5
            md5 = hashlib.md5()
            md5.update(output)
            result[project][host]['output'] = output
            result[project][host]['md5'] = md5.hexdigest()

            q.task_done()
        return True

    # 工作线程数量, 配置文件无定义则默认1
    worker_num = f.getint('thread', 'num') if f.has_option('thread', 'num') else 1

    result = {i: {} for i in projects_list}

    for i in range(worker_num):
        logging.debug('start work')
        worker = Thread(target=consumer, args=(q, result))
        worker.setDaemon(True)
        worker.start()

    q.join()

    for p, v in result.items():
        md5_list = [(h, mv['md5']) for h, mv in v.items()]
        logging.info('string md5 {}'.format(md5_list))
        if len(md5_list) > 1:
            for h, m in md5_list:
                # 找出项目内与其他主机不同的MD5值. MD5值不同表示ssh输出的所有文件MD5中有与其他主机不相同的地方
                if [x[1] for x in md5_list].count(m) <= 1:
                    logging.error('find different md5! Project: [{}] Host: [{}] MD5:[{}]'.format(p, h, m))

                    # 进一步查找具体不同的文件
                    all_file_md5 = [(h2, mv['output'].split('\n')) for h2, mv in v.items()]
                    same_file = reduce(lambda x, y: set(x) & set(y), [i[1] for i in all_file_md5])
                    logging.debug('same file {}'.format(same_file))
                    for hn, output in all_file_md5:
                        logging.debug('number of output {}'.format(output))
                        logging.error('>>>> Project: [{}] Host: [{}] File: {}'.format(p, hn, list(set(output) - set(same_file))))

