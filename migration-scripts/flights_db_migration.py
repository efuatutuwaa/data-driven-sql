import subprocess
import os
from dotenv import load_dotenv


class FlightDBMigration:
    def __init__(self, backup_file):
        self.backup_file = backup_file

        # load environment variables .env
        load_dotenv(dotenv_path='../.env')

        # GET RDS credentials from .env
        self.rds_host = os.getenv('RDS_HOST')
        self.rds_port = os.getenv('RDS_PORT')
        self.rds_db = os.getenv('RDS_DB_NAME')
        self.rds_user = os.getenv('RDS_USER')
        self.rds_password = os.getenv('RDS_PASSWORD')

    def sql_command(self):
        """Creates MySQL command to restore the backup to RDS
        """
        mysql_command = [
            "mysql",
            "-h", self.rds_host,
            "-u", self.rds_user,
            "-p" + self.rds_password,
            self.rds_db,
            "<", self.backup_file,
        ]
        return " ".join(mysql_command)

    def restore_to_rds(self):
        """ Restores 'flights_backup.sql' to RDS'
        """
        try:
            mysql_command = self.sql_command()

            # Run the command
            subprocess.run(mysql_command, shell=True, check=True)
            print(f"Successfully restored to {self.backup_file} to RDS {self.rds_db}")

        except subprocess.CalledProcessError as err:
            print(f"Error: {err}")


if __name__ == "__main__":
    backup_file = "../flights_backup.sql"
    migration = FlightDBMigration(backup_file)
    migration.restore_to_rds()
