import db_connecter

class CreateDB:
    def __init__(self, db_config_file, config_section_name):
        self.db_conn = db_connecter.DBConnecter(db_config_file, config_section_name)
        self.db_conn.connect()
    
    def close(self):
        self.db_conn.close()

    def create_matchs_table(self):
        if not self.db_conn.table_exist('matchs'):
            sql_str = '''create table matchs
            (id serial primary key,
            type varchar(255),
            type_url text,
            league_turn varchar(255),
            start_time timestamp without time zone,
            status varchar(255),
            home_team varchar(255) not null,
            home_team_url text not null,
            home_team_score int,
            concede_point varchar(255) not null,
            away_team_score int,
            away_team varchar(255) not null,
            away_team_url text not null,
            half_score varchar(255),
            result varchar(255),
            analysis_url text not null,
            asia_data_url text not null,
            euro_data_url text not null
            )
            '''
            self.db_conn.exe(sql_str)
            self.db_conn.commit()
            #self.db_conn.close()
            print("Create matchs table successfully")
        else:
            print("Matchs table is already exist")

    def create_euro_odds_table(self):
        if not self.db_conn.table_exist('euro_odds'):
            sql_str = '''create table euro_odds
            (id serial primary key,
            match_id bigint references matchs(id) on update cascade on delete set null,
            company varchar(255),
            result smallint,
            priodds3 real,
            priodds1 real,
            priodds0 real,
            nowodds3 real,
            nowodds1 real,
            nowodds0 real,
            prichance3 varchar(10),
            prichance1 varchar(10),
            prichance0 varchar(10),
            nowchance3 varchar(10),
            nowchance1 varchar(10),
            nowchance0 varchar(10),
            priyrr varchar(10),
            nowyrr varchar(10),
            prikelly3 real,
            prikelly1 real,
            prikelly0 real,
            nowkelly3 real,
            nowkelly1 real,
            nowkelly0 real
            )
            '''
            self.db_conn.exe(sql_str)
            self.db_conn.commit()
            #self.db_conn.close()
            print("Create euro odds table successfully")
        else:
            print("Euro odds table is already exist")

    def create_asia_odds_table(self):
        if not self.db_conn.table_exist('asia_odds'):
            sql_str = '''create table asia_odds
            (id serial primary key,
            match_id bigint references matchs(id) on update cascade on delete set null,
            company varchar(255),
            result smallint,
            priodds3 real,
            priconcede varchar(255),
            priodds0 real,
            nowodds3 real,
            nowconcede varchar(255),
            nowodds0 real
            )
            '''
            self.db_conn.exe(sql_str)
            self.db_conn.commit()
            #self.db_conn.close()
            print("Create euro odds table successfully")
        else:
            print("Euro odds table is already exist")

if __name__ == '__main__':
    db = CreateDB('db.conf', 'test')
    db.create_matchs_table()
    db.create_euro_odds_table()
    db.create_asia_odds_table()
    db.close()


