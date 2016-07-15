   select(
          (.pattern |contains("sth"))  or
          (.pattern | contains("inf")) or 
          (.pattern | contains("sth1") ) or
          (.pattern | contains("sth2") ) or
          (.pattern | contains("sth1") ) or
          (.pattern | contains("v-link") ) or
          (.pattern | contains("advp") ) or
          (.pattern | contains("adjp") ) or
          (.pattern | contains("wh") ) or
          (.pattern | contains("do") ) or
          (.pattern | contains("doing") ) or
          (.pattern | contains("done") ) or
          (.pattern | contains("one's") ) or
          (.pattern | contains("oneself") ) or
          (.pattern | contains("prep") ) or
          (.pattern | contains("-thing") ) 
)  
| .ch_patterns = (.ch_patterns | group_by(.ch_pattern) | map(sort_by(-.prob)[0]) | map(select(.prob > 1e-10))  |sort_by(-.prob)  )  
| select((.ch_patterns|length) > 0)
