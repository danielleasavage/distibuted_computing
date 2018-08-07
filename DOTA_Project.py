from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression


conf = SparkConf().setMaster("local").setAppName('DOTA_PROJECT')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

#LOAD DATA FROM MONGO DB

item_ids = spark.read.format("com.mongodb.spark.sql.DefaultSource").option('uri','mongodb://54.191.76.32/msan697.item_ids').load()

players = spark.read.format("com.mongodb.spark.sql.DefaultSource").option('uri','mongodb://54.191.76.32/msan697.players').load()

match = spark.read.format("com.mongodb.spark.sql.DefaultSource").option('uri','mongodb://54.191.76.32/msan697.match').load()

purchases = spark.read.format("com.mongodb.spark.sql.DefaultSource").option('uri','mongodb://54.191.76.32/msan697.purchase_log').load()

# EXPLORANATORY ANALYSIS

players_new = players.select(['match_id','player_slot','kills','deaths','assists','gold_per_min','xp_per_min'])

match_new = match.select(['match_id','duration', 'radiant_win'])

players_match = players_new.join(how='left_outer', on='match_id', other=match_new)

purch_new = purchases.select(['player_slot','match_id','item_id','time'])

pmatch_purch = purch_new.join(how='left_outer', on=['match_id', 'player_slot'], other=players_match)

pmatch_purch.groupBy('match_id').count().agg({'count':'avg'}).show()

pmatch_purch.groupBy('match_id').count().agg({'count':'min'}).show()

pmatch_purch.groupBy('match_id').count().agg({'count':'max'}).show()


from pyspark.sql.functions import udf
def make_win_col(pslot, radwin):
    if (pslot < 50) ^ (radwin == 'False'):
        return 1
    else:
        return 0

myAtomicUDF = udf(make_win_col, IntegerType())

pmpurch = pmatch_purch.withColumn('win', myAtomicUDF(pmatch_purch.player_slot, pmatch_purch.radiant_win))

final_pmpurch= pmpurch.drop('radiant_win','player_slot','match_id')

print final_pmpurch.count()

onehotenc = OneHotEncoder(inputCol='item_id', outputCol="itemid-onehot", dropLast=False)
one_hot_df = onehotenc.transform(final_pmpurch).drop('item_id')
one_hot_df = one_hot_df.withColumnRenamed("itemid-onehot", 'item_id')

va = VectorAssembler(outputCol='features', inputCols=sorted(one_hot_df.columns)[0:-1])
explanatory_df = va.transform(one_hot_df).select('features', one_hot_df['win'].alias('label'))

lr = LogisticRegression(regParam=0.01, maxIter=1000, fitIntercept=True)
lrmodel = lr.fit(explanatory_df)

print lrmodel.coefficients
print lrmodel.intercept

############################################
###### ATTEMPT TO DO A RANDOM FOREST ######
#####    FAILED DUE TO NUMBER OF     ######
#####       RESPONSE CATAGORIES   #########
###########################################
sqlContext.clearCache()

def safeFloat(x):
    try:
        return float(x)
    except ValueError:
        return str(x)

def preprocess(f):
    # takes a file name w/ header row and returns a DF
    pre_data = sc.textFile(f).map(lambda x:x.split(','))
    head = pre_data.first()
    body = pre_data.filter(lambda x:x[0] != head[0]).map(lambda x:[-1.0 if len(xi)==0 or xi=='null' else safeFloat(xi) for xi in x])
    return body.toDF(head)

playersS3=preprocess('s3n://dota-ds-jr-ss-fh/players.csv')

players_for_preds = playersS3.drop('account_id','unit_order_patrol', 'unit_order_radar','unit_order_vector_target_position', 'unit_order_set_item_combine_lock', 'unit_order_continue', 'unit_order_cast_rune', 'unit_order_move_to_direction', 'unit_order_eject_item_from_stash', 'unit_order_taunt', 'unit_order_cast_toggle_auto', 'unit_order_disassemble_item', 'unit_order_none', 'gold_abandon').withColumn('hero_ids',1*playersS3.hero_id).drop('hero_id')

final_hero_df = players_for_preds.select('kills', 'deaths', 'assists', 'denies','last_hits', 'xp_per_min', 'gold_per_min', 'gold_spent', 'gold','hero_damage','tower_damage','item_0','item_1','item_2','item_3','item_4','item_5','level','hero_ids')

va2 = VectorAssembler(outputCol='features', inputCols=final_hero_df.columns[:-1])
predictive_df2 = va2.transform(final_hero_df).select('features', final_hero_df['hero_ids'].alias('label'))

splits = predictive_df2.randomSplit([0.9, 0.1])
split2 = splits[0].randomSplit([0.8, 0.2])
train = splits[0].cache()
valid = split2[1].cache()
test = splits[1].cache()


#rf = RandomForestClassifier(labelCol='label', featuresCol='features')
#hero_model = rf.fit(train)

#prediction = hero_model.transform(valid)
#evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
#hero_f1 = evaluator.evaluate(prediction)

sc.stop()