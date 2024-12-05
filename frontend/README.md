# Steps to use

1. Go to the project directory and run:
> npm install

> node app.js $KAFKABROKERS 3076

2. Configure proxy settings (in Firefox):
![Proxy Settings](./firefox_proxy.png)

3. Run on your local cluster:
> ssh -i \<ssh private key> -C2qTnNf -D 9876 sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net

4. Access:
> http://10.0.0.38:3076/
