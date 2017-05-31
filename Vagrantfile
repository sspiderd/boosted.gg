# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure(2) do |config|
  
  config.vm.box = "ubuntu/xenial64"

  #config.vm.box = "ubuntu/xenial-cloud"
  #config.vm.box_url = "http://cloud-images.ubuntu.com/xenial/current/xenial-server-cloudimg-amd64-vagrant.box"

  #config.vm.box_version = "20160830.0.0"

  config.vm.provider "virtualbox" do |v|
   #v.customize ["modifyvm", :id, "--nictype1", "virtio"]
   v.customize ["modifyvm", :id, "--cpuexecutioncap", "80"]
   v.memory = 2048
   v.cpus = 1
  end

  config.vm.network "private_network", ip: "10.0.0.3" , nic_type: "virtio"

  #For the day i'll be using docker for kafka and zk
  #config.vm.network "forwarded_port", guest: 9092, host: 9092
  #config.vm.network "forwarded_port", guest: 2181, host: 2181

  ["docker-containers", "transparent_hugepage"].each do |service|
  #I have to copy it to /tmp and later copy it with the provision_late.sh since the "file" provisioner is done with the vagrant user, rather than root
    config.vm.provision "file", source: "provision/#{service}.service", destination: "/tmp/#{service}.service"
  end
  config.vm.provision "file", source: "provision/cassandra.init", destination: "/tmp/cassandra.init"

  config.vm.provision "shell", path: "provision/provision.sh"

end
