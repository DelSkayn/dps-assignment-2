
reserve_line=`preserve -llist | grep ddps2015 | cut -f 9`
echo $reserve_line
head_node=`echo $reserve_line | awk "{print $1}"`
rest_nodes=`echo $reserve_line | cut -f 2`
echo "${reserve_line[1]}"

