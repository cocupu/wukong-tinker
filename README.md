
# Examples

Use regular wukong to parse a json file and run its contents through a processor.
`cat data/gnip_twitter_2013_12_15_00_00.json | bundle exec wu-local --from=json extractor.rb`

Use wukong-hadoop to analyze a text file using the mapper & reducer defined in the given Ruby file.  Run locally (instead of on hadoop.)
`bundle exec wu-hadoop hadoop-examples/word_count.rb --mode=local --input=data/howl.txt `

Use wukong-hadoop to parse a json file using the mapper & reducer defined in the given Ruby file.  Run it locally.
`bundle exec wu-hadoop --from=json twitter_url_extractor.rb --mode=local --input=data/gnip_twitter_2013_12_15_00_00.json`

Use wukong-hadoop to parse all of the json files in a directory using the mapper & reducer defined in the given Ruby file.  Run locally.
`cat ../mjg_twitter/json/*.json |  bundle exec wu-hadoop --from=json twitter_url_extractor.rb --mode=local`