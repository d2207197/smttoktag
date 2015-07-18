map(
   select(
          (.en_pattern |contains("sth"))  or
          (.en_pattern | contains("inf")) or 
          (.en_pattern | contains("sth1") ) or
          (.en_pattern | contains("sth2") ) or
          (.en_pattern | contains("sth1") ) or
          (.en_pattern | contains("v-link") ) or
          (.en_pattern | contains("advp") ) or
          (.en_pattern | contains("adjp") ) or
          (.en_pattern | contains("wh") ) or
          (.en_pattern | contains("do") ) or
          (.en_pattern | contains("doing") ) or
          (.en_pattern | contains("done") ) or
          (.en_pattern | contains("one's") ) or
          (.en_pattern | contains("oneself") ) or
          (.en_pattern | contains("prep") ) or
          (.en_pattern | contains("-thing") ) 

))  
| map( .ch_patterns = (.ch_patterns | group_by(.ch_pattern) | map(sort_by(-.prob)[0]) | map(select(.prob > 1e-14))  |sort_by(-.prob)  ) ) 
