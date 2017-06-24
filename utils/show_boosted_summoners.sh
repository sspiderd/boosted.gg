vagrant ssh -c "docker exec cassandra /usr/bin/cqlsh -e 'select champion, role, summoner_name, platform, rank, winrate, games_played from boostedgg.boosted_summoners'"
#cqlsh 10.0.0.3 -e "select * from boostedgg.boosted_summoners_by_champion_and_role"
