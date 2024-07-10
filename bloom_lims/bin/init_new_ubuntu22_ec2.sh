
# Create an ubuntu 22.04 AMI instance, using 8-16vCPU and reasonable memory and 60BG disk (this is to get started, 
#  we can always increase later).  I have only tested on x86_64, but arm64 should work too. 
# Launch your instance ** Make sure the ssh port is open in your AWS settings, as well as port 8911 and 8118. http and https, ssh anb dns are good ideas too.

# ssh into the box ssh -i .pem ubuntu@ip
ssh-keygen # defaults


# first, add ~/.ssh/id_rsa.pub to github.
# add some system packages
sudo apt-get update
sudo apt-get install -y htop glances atop emacs tmux git rclone fd-find # this may take 10min & when the ncurses menu pops up, i just acepted defaults.


# Create place to clone repos
mkdir -p projects/git && projects/git

# Clone bloom & move into it
git clone git@github.com:Daylily-Informatics/bloom.git
cd bloom


 ##
#  Add Some Config Files ( see each repo for details on what goes in them)
 ##

# ~/.aws/{config,credentials}
# and
#  ~/.config/fedex/fedex_prod.yaml

 ##
#  From here we follow the README installation section.
 ##

# Install CONDA for the user
cd ~
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh #accept all defaults


# The last option gave permission to modify your .bashrc so conda is available on your future new shells.  The shell you are in does not have conda active.
# logout and logback in

###
exit
# Then ssh  BACK TO THE BOX AGAIN
###

# You now should see a '(base)' to the left of your prompt.  This means conda is active.  If you don't see it, you can activate it with `conda activate base`   

cd projects/git/bloom/


 ##
#  creating the .env file (see the supabase.md doc for details)
 ##

# This .env file is created in the bloom root dir.
# Set up supabase to get the SUPABASE_KEY & SUPABASE_URLUPABASE_* .env variables
# Specify all domains which users can authenticate into the system by setting the SUPABASE_WHITELIST_DOMAINS .env variable. =all will not filter, google.com,yahoo.com will only allow google and yahoo domains in. No spaces in this string.
# Specify the S3 bucket prefix to be used to find buckets dewey can use.
# please read the supabase.md file for more details.


# Install the BLOOM conda environment
source bloom_lims/env/install_postgres.sh # this can take a few min to 10+min, it depends on the hardware.