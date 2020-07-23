#coding=utf-8
import sys, os, pwd, grp, signal, time, glob
from resource_management import *
from subprocess import call





class Master(Script):
  #通过ambari安装 yum install ark-data-api*
  def install(self, env):
    import params
    import status_params

    #清除原先的安装包
    #e.g. /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/FLINK/package
    service_packagedir = os.path.realpath(__file__).split('/scripts')[0]
    Execute('rm -rf ' + params.flink_install_dir, ignore_failures=True)
            
    Directory([status_params.flink_pid_dir, params.flink_log_dir, params.flink_install_dir],
            owner=params.flink_user,
            group=params.flink_group
    )   

    File(params.flink_log_file,
            mode=0644,
            owner=params.flink_user,
            group=params.flink_group,
            content=''
    )

         
    
    #User selected option to use prebuilt flink package 
    if params.setup_prebuilt:
      #此时配置为true
      Execute('echo Installing packages')
      #Fetch and unzip snapshot build, if no cached flink tar package exists on Ambari server node
      if not os.path.exists(params.temp_file):
        #flink_download_url：http://192.168.193.129/flink/flink-1.9.0-bin-scala_2.11.tgz
        #temp_file:'/tmp/flink.tgz'
        #flink_log_dir：/var/log/flink
        #flink_log_file： os.path.join(flink_log_dir,'flink-setup.log')
        #flink_user：flink
        #下载压缩包
        #wget http://192.168.193.129/flink/flink-1.9.0-bin-scala_2.11.tgz -O /tmp/flink.tgz -a /var/log/flink/flink-setup.log (user=flink)
        Execute('wget '+params.flink_download_url+' -O '+params.temp_file+' -a '  + params.flink_log_file, user=params.flink_user)

        #flink_install_dir:/opt/flink
        #解压压缩包
        #tar -zxvf /tmp/flink.tgz -C /opt/flink >> /var/log/flink/flink-setup.log (user=flink)
        Execute('tar -zxvf '+params.temp_file+' -C ' + params.flink_install_dir + ' >> ' + params.flink_log_file, user=params.flink_user)

        #移动压缩包
        #mv /opt/flink/*/* /opt/flink
        Execute('mv '+params.flink_install_dir+'/*/* ' + params.flink_install_dir, user=params.flink_user)
                
      #update the configs specified by user
      self.configure(env, True)

      
    else:
      ##此时配置为true，这里不用看了
      #User selected option to build flink from source
       
      #if params.setup_view:
        #Install maven repo if needed
      self.install_mvn_repo()      
      # Install packages listed in metainfo.xml
      self.install_packages(env)    
    
      
      Execute('echo Compiling Flink from source')
      Execute('cd '+params.flink_install_dir+'; git clone https://github.com/apache/flink.git '+params.flink_install_dir +' >> ' + params.flink_log_file)
      Execute('chown -R ' + params.flink_user + ':' + params.flink_group + ' ' + params.flink_install_dir)
                
      Execute('cd '+params.flink_install_dir+'; mvn clean install -DskipTests -Dhadoop.version=2.7.1.2.3.2.0-2950 -Pvendor-repos >> ' + params.flink_log_file, user=params.flink_user)
      
      #update the configs specified by user
      self.configure(env, True)

  
  #安装完成后，做的一些配置信息，比如根据模版信息生成相关文件、生成相关的文件夹，改变用户组和权限等等
  def configure(self, env, isInstall=False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)
    
    self.set_conf_bin(env)

    #Applying File['/opt/flink/conf/flink-conf.yaml'] failed, parent directory /opt/flink/conf doesn't exist
    #params.flink_install_dir = /opt/flink
    #这里tm的已经存在了。。。。卧槽
    #Execute('mkdir ' + params.flink_install_dir + '/conf', user=params.flink_user)

    if os.path.exists(params.flink_install_dir)==False:
      os.makedirs(params.flink_install_dir)
    elif os.path.exists(params.flink_install_dir + '/conf')==False:
      os.makedirs(params.flink_install_dir + '/conf')


        
    #write out nifi.properties
    properties_content=InlineTemplate(params.flink_yaml_content)
    File(format("{conf_dir}/flink-conf.yaml"), content=properties_content, owner=params.flink_user)


  #如何关停服务。我这也是脚本
  def stop(self, env):
    import params
    import status_params    
    from resource_management.core import sudo

    #通过pid文件来杀掉进程
    #/var/run/flink/flink.pid
    pid = str(sudo.read_file(status_params.flink_pid_file))
    Execute('yarn application -kill ' + pid, user=params.flink_user)

    Execute('rm ' + status_params.flink_pid_file, ignore_failures=True)
 
  #如何启动服务。我这是先检测服务允许状态，再通过脚本启动
  # （这个脚本我是打包到rpm包里面的，安装时就放到相关的文件夹中了。而且脚本有维护pid操作）
  def start(self, env):
    import params
    import status_params
    self.set_conf_bin(env)  
    self.configure(env) 

    #flink
    self.create_hdfs_user(params.flink_user)

    #这两句就是显示一下
    #''
    Execute('echo bin dir ' + params.bin_dir)
    #/var/run/flink/flink.pid
    Execute('echo pid file ' + status_params.flink_pid_file)

    cmd = format("export HADOOP_CONF_DIR={hadoop_conf_dir}; {bin_dir}/yarn-session.sh -n {flink_numcontainers} -s {flink_numberoftaskslots} -jm {flink_jobmanager_memory} -tm {flink_container_memory} -qu {flink_queue} -nm {flink_appname} -d")
    #这里是false，不用看了
    if params.flink_streaming:
      cmd = cmd + ' -st '

    #先注册环境变量
    #export HADOOP_CONF_DIR=/etc/hadoop/conf;

    #然后执行
    # /yarn-session.sh -n 1 -s 1 -jm 768 -tm 1024 -qu default -nm flinkapp-from-ambari -d
    # >> /var/log/flink/flink-setup.log (user=flink)
    Execute (cmd + format(" >> {flink_log_file}"), user=params.flink_user)

    # 参考  echo "aa bb cc" | awk -F '{print $1}' 结果就是aa，意思是把字符串按空格分割，取第一个
    #yarn application -list 2>/dev/null | awk '/flinkapp-from-ambari/ {print $1}' | head -n1 > /var/run/flink/flink.pid
    Execute("yarn application -list 2>/dev/null | awk '/" + params.flink_appname + "/ {print $1}' | head -n1 > " + status_params.flink_pid_file, user=params.flink_user)
    #Execute('chown '+params.flink_user+':'+params.flink_group+' ' + status_params.flink_pid_file)

    #temp_file:'/tmp/flink.tgz'
    if os.path.exists(params.temp_file):
      os.remove(params.temp_file)

  def check_flink_status(self, pid_file):
    from datetime import datetime 
    from resource_management.core.exceptions import ComponentIsNotRunning
    from resource_management.core import sudo
    from subprocess import PIPE,Popen
    import shlex, subprocess
    #/var/run/flink/flink.pid
    if not pid_file or not os.path.isfile(pid_file):
      raise ComponentIsNotRunning()
    try:
      pid = str(sudo.read_file(pid_file)) 
      cmd_line = "/usr/bin/yarn application -list"
      #shlex.split()可以被用于序列化复杂的命令参数
      #>>> shlex.split('ls ps top grep pkill')
      #['ls', 'ps', 'top', 'grep', 'pkill']
      args = shlex.split(cmd_line)
      proc = Popen(args, stdout=PIPE)
      p = str(proc.communicate()[0].split())
      if p.find(pid.strip()) < 0:
        raise ComponentIsNotRunning() 
    except Exception, e:
      raise ComponentIsNotRunning()

  # 通过检测pid文件查询服务状态。（上面的服务启停脚本维护了pid文件）
  def status(self, env):
    import status_params       
    from datetime import datetime
    #/var/run/flink/flink.pid
    self.check_flink_status(status_params.flink_pid_file)

  def set_conf_bin(self, env):
    import params
    if params.setup_prebuilt:
      params.conf_dir =  params.flink_install_dir+ '/conf'
      params.bin_dir =  params.flink_install_dir+ '/bin'
    else:
      params.conf_dir =  glob.glob(params.flink_install_dir+ '/flink-dist/target/flink-*/flink-*/conf')[0]
      params.bin_dir =  glob.glob(params.flink_install_dir+ '/flink-dist/target/flink-*/flink-*/bin')[0]
    

  def install_mvn_repo(self):
    #for centos/RHEL 6/7 maven repo needs to be installed
    distribution = platform.linux_distribution()[0].lower()
    if distribution in ['centos', 'redhat'] and not os.path.exists('/etc/yum.repos.d/epel-apache-maven.repo'):
      Execute('curl -o /etc/yum.repos.d/epel-apache-maven.repo https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo')

  def create_hdfs_user(self, user):
    Execute('hadoop fs -mkdir -p /user/'+user, user='hdfs', ignore_failures=True)
    Execute('hadoop fs -chown ' + user + ' /user/'+user, user='hdfs')
    Execute('hadoop fs -chgrp ' + user + ' /user/'+user, user='hdfs')
          
if __name__ == "__main__":
  Master().execute()
