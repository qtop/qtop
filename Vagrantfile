ENV['VAGRANT_DEFAULT_PROVIDER'] = "docker"

Vagrant.configure("2") do |config|

	# -------------- PBS server --------------

	config.vm.define "pbs" do |pbs|
	
		pbs.vm.provider "docker" do |d|
			d.image = "agaveapi/torque"
			d.name = "pbs"
			d.privileged = true
			d.vagrant_machine = "qtop-docker-provider"
			d.vagrant_vagrantfile = "./Vagrantfile.dockerhost"
		end
		
		pbs.vm.synced_folder ".", "/home/testuser/qtop", mount_options: ["dmode=777,fmode=666"]
		pbs.vm.hostname = "pbs"
	end

	config.ssh.username = 'testuser'
end
