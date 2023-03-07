# !/bin/sh

propsFile=$1
localhost=$2
repFile=$3
query=$4
batch=$((9+($5*10)))

endpoints=$(seq -s " " -f "http://www.ratingsite%01g.fr/" 0 $batch; seq -s " " -f "http://www.vendor%01g.fr/" 0 $batch)
nbendpoints=$((($batch+1)*2))

if [ $6 = true ]; then
    echo "Generating summaries for $nbendpoints endpoints..."
    start=`date +%M`
    java -classpath "costfed/target/costfed-core-0.0.1-SNAPSHOT.jar:costfed/target/*" org.aksw.simba.quetsal.util.TBSSSummariesGenerator $localhost costfed/summaries/sum.n3 $endpoints >> /dev/null
    end=`date +%M`
    runtime=$((end-start))
    echo "Generating summaries for $nbendpoints endpoints took $runtime min"
fi

if [ $7 = true ]; then
    echo "Execute query $query for $nbendpoints endpoints..."
    java -classpath "costfed/target/costfed-core-0.0.1-SNAPSHOT.jar:costfed/target/*" org.aksw.simba.start.QueryEvaluation $propsFile $localhost $repFile $query $endpoints >> /dev/null
    echo "Execute query $query for $nbendpoints endpoints is done!"
fi