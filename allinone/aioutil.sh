#!/bin/bash
set -e

export GOVERSION=1.6

echo "Using $GIT_USER as user"
echo "Setting up dev env"

apt-get update
apt-get install -y --force-yes vim git build-essential autoconf libtool libtool-bin unzip fuse mercurial
update-alternatives --set editor /usr/bin/vim.basic

# setup grpc
# echo deb http://http.debian.net/debian jessie-backports main >> /etc/apt/sources.list
apt-get update
apt-get install libgrpc-dev -y --force-yes

# setup go
if [ ! -d "/$USER/go/bin" ]; then
    mkdir -p /$USER/go/bin
    cd /tmp &&  wget -q https://storage.googleapis.com/golang/go$GOVERSION.linux-amd64.tar.gz
    tar -C /usr/local -xzf /tmp/go$GOVERSION.linux-amd64.tar.gz
    echo " " >> /$USER/.bashrc
    echo "# Go stuff" >> /$USER/.bashrc
    echo "export PATH=\$PATH:/usr/local/go/bin" >> /$USER/.bashrc
    echo "export GOPATH=/root/go" >> /$USER/.bashrc
    echo "export PATH=\$PATH:\$GOPATH/bin" >> /$USER/.bashrc
    source /$USER/.bashrc
fi

# sup3r sekret option to install vim-go and basic vim-go friendly .vimrc
if [ "$FANCYVIM" = "yes" ]; then
    echo "Performing fancy vim install"
    apt-get install vim-nox -y --force-yes
    update-alternatives --set editor /usr/bin/vim.nox
    mkdir -p ~/.vim/autoload ~/.vim/bundle && curl -LSso ~/.vim/autoload/pathogen.vim https://tpo.pe/pathogen.vim
    go get golang.org/x/tools/cmd/goimports
    git clone https://github.com/fatih/vim-go.git ~/.vim/bundle/vim-go
    git clone https://github.com/Shougo/neocomplete.vim.git ~/.vim/bundle/neocomplete.vim
    curl -o ~/.vimrc https://raw.githubusercontent.com/getcfs/cfs-binary-release/master/allinone/.vimrc
    echo "let g:neocomplete#enable_at_startup = 1" >> ~/.vimrc
    go get github.com/nsf/gocode
    echo "Fancy VIM install complete. You may way want to open vim and run ':GoInstallBinaries' the first time you use it"
    sleep 1
else
    echo "You didn't set FANCYVIM=yes so no awesome vim-go setup for you."
fi

# setup protobuf
if [ "$BUILDPROTOBUF" = "yes" ]; then
    echo "Building with protobuf support, this gonna take awhile"
    cd $HOME
    git clone https://github.com/google/protobuf.git
    cd protobuf
    ./autogen.sh && ./configure && make && make check && make install && ldconfig
    echo "Protobuf build done...hopefully"
else
    echo "Built withOUT protobuf"
fi

echo "Setting up the imporant bits..."
go get google.golang.org/grpc
go get github.com/golang/protobuf/proto
go get github.com/golang/protobuf/protoc-gen-go
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/protoc-gen-gogo
go get github.com/gogo/protobuf/gogoproto
go get github.com/gogo/protobuf/protoc-gen-gofast
go get github.com/tools/godep
go install github.com/tools/godep

# Adding some helpful git stuff to the .bashrc
if [ "$FANCYPROMPT" = "yes" ]; then
    echo "" >> ~/.bashrc
    echo "# Added to show git branches" >> ~/.bashrc
    echo 'export PS1="\u@\h \W\[\033[37m\]\$(git_branch)\[\033[00m\] $ "' >> ~/.bashrc
    echo '' >> ~/.bashrc
    echo '# get the current git branch' >> ~/.bashrc
    echo 'git_branch() {' >> ~/.bashrc
    echo "        git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/ (\1)/'" >> ~/.bashrc
    echo '    }' >> ~/.bashrc
fi

echo
echo "If you plan on using *THIS* session and to get the git enhanced prompt make sure to source ~/.bashrc to load path changes"
