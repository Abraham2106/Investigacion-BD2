#!/bin/bash
 
sudo apt update && sudo apt install -y dos2unix
mkdir proyectos 
cd ~/proyectos/
code ./
# Recuerde copiar el proyecto original 
# lo pasa  manualmente 
cd Investigacion-BD2
 
dos2unix start.sh
dos2unix stop.sh
chmod +x start.sh
chmod +x stop.sh
 
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
 
pip install --upgrade pip
pip install -r requirements.txt
 
sudo apt install -y openjdk-17-jdk openjdk-17-jre
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
 
./start.sh
 
