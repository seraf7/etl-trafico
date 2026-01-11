import subprocess

def upload_to_hdfs(local_path, hdfs_path):
    cmd = ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path]
    # cmd = ["hadoop", "fs", "-put", "-f", local_path, hdfs_path]
    subprocess.run(cmd, check=True)

def create_path_hdfs(hdfs_path):
    cmd = ["hdfs", "dfs", "-mkdir", "-p", hdfs_path]
    subprocess.run(cmd, check=True)