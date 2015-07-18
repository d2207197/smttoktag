map( .ch_patterns = (.ch_patterns | group_by(.ch_pattern) | map(sort_by(-.prob)[0]) | map(select(.prob > 1e-14))  |sort_by(-.prob)  ) ) 
