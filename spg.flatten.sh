jq 'map({en_pattern: .en_pattern }  + .ch_patterns[])[] | "\t" + .en_pattern + "\t" + .ch_pattern + "\t" + (.prob | tostring) '  | parallel -k -j 100  "echo {} | sed 's/\"//g'" 
