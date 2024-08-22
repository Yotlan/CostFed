# !/bin/sh

propsFile=$1
endpoints=$2
resultFile=$3
provenanceFile=$4
explainFile=$5
#statFile=$6
timeout=$6
query=$7
batch_id=$8
generateSummaries=$9
executeQuery=$10
summary=$11
noExec=$12

sed -Ei "s#quetzal.fedSummaries=.*#quetzal.fedSummaries=$summary#g" costfed/costfed.props
sed -Ei "s#quetzal.fedSummaries=.*#quetzal.fedSummaries=$summary#g" costfed/fedx.props

if [ $generateSummaries = true ]; then
    echo "Generating summary for batch $batch_id..."
    start=`date +%M`
    java -classpath "costfed/target/costfed-core-0.0.1-SNAPSHOT.jar:costfed/target/*" org.aksw.simba.quetsal.util.TBSSSummariesGenerator $summary $endpoints >> /dev/null
    end=`date +%M`
    runtime=$((end-start))
    status=$?
    if [ $status -eq 0 ]; then
        echo "Generating summaries for batch $batch_id took $runtime min"
    else
        exit $status
    fi    
fi

if [ $executeQuery = true ]; then
    echo "Execute query $query for batch $batch_id endpoints..."
    java -classpath "costfed/target/costfed-core-0.0.1-SNAPSHOT.jar:costfed/target/*" org.aksw.simba.start.QueryEvaluation $propsFile $resultFile $provenanceFile $explainFile $timeout $summary $query $noExec $endpoints #>> /dev/null
    status=$?
    if [ $status -eq 0 ]; then
        echo "Execute query $query for batch $batch_id endpoints is done!"
    fi

    exit $status
fi