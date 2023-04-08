DATABASE_NAME=$1
if [ -z "$DATABASE_NAME" ]
then
    printf "\n[FAILURE]: No database name supplied\n\n"
    echo "Exiting in 10 seconds..."
    sleep 10
    exit 0
fi
export DATABASE_NAME

echo "alter user postgres with password 'admin';
create database $DATABASE_NAME;" > create_database.sql

cat create_database.sql | sudo -iu postgres psql

printf "\n[SUCCESSFUL]: Database::$DATABASE_NAME Creation\n\n"

rm create_database.sql

printf "\n[SUCCESSFUL]: Cleanup\n\n"