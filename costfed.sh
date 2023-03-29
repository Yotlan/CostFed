# !/bin/sh

propsFile=$1
localhost=$2
resultFile=$3
provenanceFile=$4
explainFile=$5
statFile=$6
timeout=$7
query=$8
batch_id=$9
batch=$((9+($batch_id*10)))

generateSummaries=$10
executeQuery=$11
summary=$12

endpoints=$(seq -s " " -f "http://www.ratingsite%01g.fr/" 0 $batch; seq -s " " -f "http://www.vendor%01g.fr/" 0 $batch)
nbendpoints=$((($batch+1)*2))

sed -Ei "s#quetzal.fedSummaries=.*#quetzal.fedSummaries=$summary#g" costfed/costfed.props

if [ $generateSummaries = true ]; then
    echo "Generating summaries for $nbendpoints endpoints..."
    start=`date +%M`
    java -classpath "costfed/target/costfed-core-0.0.1-SNAPSHOT.jar:costfed/target/*" org.aksw.simba.quetsal.util.TBSSSummariesGenerator $localhost $summary $endpoints >> /dev/null
    end=`date +%M`
    runtime=$((end-start))
    echo "Generating summaries for $nbendpoints endpoints took $runtime min"
fi

if [ $executeQuery = true ]; then
    echo "Execute query $query for $nbendpoints endpoints..."
    echo "$summary"
    #echo "java -classpath "costfed/target/costfed-core-0.0.1-SNAPSHOT.jar:costfed/target/*" org.aksw.simba.start.QueryEvaluation $propsFile $localhost $resultFile $provenanceFile $explainFile $statFile $timeout $query $endpoints >> /dev/null"
    java -classpath "costfed/target/costfed-core-0.0.1-SNAPSHOT.jar:costfed/target/*" org.aksw.simba.start.QueryEvaluation $propsFile $localhost $resultFile $provenanceFile $explainFile $statFile $timeout $summary $query $endpoints >> /dev/null
    echo "Execute query $query for $nbendpoints endpoints is done!"
fi