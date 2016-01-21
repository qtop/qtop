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

		pbs.vm.synced_folder ".", "/home/testuser/qtop"
		pbs.vm.hostname = "pbs"
	end

    # -------------- SGE server --------------
	config.vm.define "sge" do |sge|

		sge.vm.provider "docker" do |d|
			d.image = "agaveapi/gridengine"
			d.name = "sge"
			d.privileged = true
			d.vagrant_machine = "qtop-docker-provider"
			d.vagrant_vagrantfile = "./Vagrantfile.dockerhost"
		end

		sge.vm.synced_folder ".", "/home/testuser/qtop"
		sge.vm.hostname = "sge"
	end

	config.ssh.username = 'testuser'

end
