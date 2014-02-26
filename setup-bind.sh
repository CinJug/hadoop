echo "Fix IPs"
exit 0

touch ~/.ssh/known_hosts
if grep "master.hadoop-cinjug" ~/.ssh/known_hosts > /dev/null
then
    echo "already in known hosts"
else
    echo "master.hadoop-cinjug ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAr9CYuF/r9Jg5WXlbqnM3YEnyf6oCegzlp5tZdXmjrGftEYSV70gHrySHp0OdRn27jdJdxJwAA5Frcrj6Daa/7JBgnjZWdU6qCGnGmQG52DouYjc7UhLfTge2iWb3GcMJsWk35aQUBEzdSc9u0pzogHG8ACyFGggG0XBLjbuajkqnoHS74enQqP0MD8qqrJALyHoq7rWzaKKIJOovlxovJurTKL137Cc8o3b+QL/Bupup85CGu4FInre+0BhSxyasGuXxVExYMwDQy2vkwC3h+9yQCLaO9bQwJdlFGPGCTmXDgFbkfq7SoCmREn1KhRiLEsmCjxdkgc7hQn+K5gnabQ==" >> ~/.ssh/known_hosts
fi

sudo cp /root/.ssh/id_rsa /home/hadoop/.ssh
sudo cp /root/.ssh/id_rsa.pub /home/hadoop/.ssh
sudo chown hadoop:hadoop /home/hadoop/.ssh/id_rsa*

cur_ip=`ifconfig eth0 | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p'`

sudo bash -c "echo $'domain hadoop-cinjug\nsearch hadoop-cinjug\nnameserver 192.168.x.x' > /etc/resolv.conf"

D_NAME=`ssh bind_serv@master.hadoop-cinjug $cur_ip`
sudo hostname $D_NAME.hadoop-cinjug
