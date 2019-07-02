import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PSO_RDD {
  
  var dimension = 2
  var no_of_particles =100
  var no_of_iteration = 100
  var gbest_position=Array.fill(dimension)(math.random)
    
  def main(args:Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("PSO_RDD").setMaster("spark://abhi8569:7077").set("spark.eventLog.enabled","true")
              .set("spark.eventLog.dir","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
              .set("spark.history.fs.logDirectory","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
    val sc = new SparkContext(conf)
    
    var swarm = ArrayBuffer[Particle]()
    for(i <- 0 to no_of_particles-1){
      swarm += new Particle(dimension,i) with Serializable
    }
    var newSwarm = swarm.map(x => init_pbest(x))
    newSwarm.map(f => global_best_position(f.p_best))
    var swarm_rdd = sc.parallelize(newSwarm,8)
        
    var temp_rdd: RDD[Particle]= sc.emptyRDD[Particle]
    temp_rdd=swarm_rdd    
    var updated_velocity_swarm: RDD[Particle]= sc.emptyRDD[Particle]
    
    updated_velocity_swarm = swarm_rdd.map(x => update_particle(x,no_of_iteration))
    updated_velocity_swarm.count()  //dummy action to trigger map
    temp_rdd = updated_velocity_swarm
    
    def update_particle(p:Particle,iteration:Int):Particle={
      for(it <- 0 to iteration-1){
      
        for(i <- 0 to dimension-1){
          var toward_pbest= math.random*(p.p_best(i)-p.p_position(i))
          var toward_gbest =  2*math.random*(gbest_position(i)-p.p_position(i))
          p.p_velocity(i) = 0.5*p.p_velocity(i) + toward_pbest + toward_gbest
        }
        for(i <- 0 to dimension-1){
          p.p_position(i) = p.p_position(i) + p.p_velocity(i)
        }
        
        if(obj_func(p.p_position) < obj_func(p.p_best)){
          p.p_best = p.p_position
        }
        
        if(obj_func(p.p_best) < obj_func(gbest_position)){
          gbest_position=p.p_position
        }        
      }
      println(iteration+" => Best value after iteration of particle ["+p.p_id+"] is : ",obj_func(gbest_position))
      return p
    }
    
    
    //println(" Best value after each iteration  : ",obj_func(gbest_position))
    
    sc.stop()
  }
  
  val obj_func = (x:Array[Double] ) => 5 + (1/(math.pow(x(0),2) + math.pow(x(1),2)))
  
  def global_best_position(pos:Array[Double])={
    if(obj_func(pos) < obj_func(gbest_position)){
      gbest_position=pos
    }
  }  
  
  def init_pbest(p:Particle):Particle={
    p.p_best = p.p_position
    return p
  }  
}