inputFile = LOAD 'hdfs:///user/root/episodeIV_dialouges.txt' USING PigStorage('\t') AS(name:chararray, line:chararray);

ranked = RANK inputFile;

onlyDialouges = FILTER ranked BY (rank_inputFile > 2);

groupByName = GROUP onlyDialouges BY name;

names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;

namesOrdered = ORDER names BY no_of_lines DESC;

STORE namesOrdered INTO 'hdfs:///user/root/outputs/lineResults';