
# Install PostgreSQL
# -----------------------------------------------------------------
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql

echo "alter user postgres with password 'admin';" > install_database.sql

cat install_database.sql | sudo -iu postgres psql
rm install_database.sql

printf "\nPostgres Installation: [SUCCESSFUL]\n\n"

