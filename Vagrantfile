# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure(2) do |config|
  
  config.vm.box = "ubuntu/trusty64"
  #config.vm.box = "puppetlabs/debian-8.2-64-nocm"

  config.vm.provider "virtualbox" do |v|
   v.customize ["modifyvm", :id, "--nictype1", "virtio"]
   v.customize ["modifyvm", :id, "--cpuexecutioncap", "80"]
   v.memory = 4096
   v.cpus = 4
  end

  config.vm.network "private_network", ip: "10.0.0.3", nic_type: "virtio"
  #config.vm.network "forwarded_port", guest: 9092, host: 9092
  #config.vm.network "forwarded_port", guest: 2181, host: 2181

  config.vm.provision "shell", path: "provision/provision.sh"
  config.vm.provision "file", source: "provision/zookeeper.conf", destination: "/tmp/zookeeper.conf"
  config.vm.provision "file", source: "provision/kafka.conf", destination: "/tmp/kafka.conf"
  config.vm.provision "shell", path: "provision/provision_late.sh"

end
