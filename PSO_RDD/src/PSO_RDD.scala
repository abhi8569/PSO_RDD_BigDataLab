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
  var no_of_particles =2
  var no_of_iteration = 10
  var gbest_position=Array.fill(dimension)(math.random)
    
  def main(args:Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("PSO_RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    var swarm = ArrayBuffer[Particle]()
    for(i <- 0 to no_of_particles-1){
      swarm += new Particle(dimension) with Serializable
    }
    var swarm_rdd = sc.parallelize(swarm)

    swarm_rdd.foreach(f => global_best_position(f.p_position))
    swarm_rdd.foreach(f => init_pbest(f))
    swarm_rdd.count()
    var temp_rdd: RDD[Particle]= sc.emptyRDD[Particle]
    temp_rdd=swarm_rdd
    var updated_velocity_swarm: RDD[Particle]= sc.emptyRDD[Particle]
    
    for(iteration <- 0 to no_of_iteration){
      updated_velocity_swarm = temp_rdd.map{x => update_particle(x)}
      updated_velocity_swarm.count()  //dummy action to trigger map
      temp_rdd = updated_velocity_swarm
      println("Best value after each iteration  : ",obj_func(gbest_position))
    }
    
    def update_particle(p:Particle):Particle={
      
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
        println("Best value after each particle update : ",obj_func(gbest_position))
      }
      return p
    }
    sc.stop()
  }
  
  val obj_func = (x:Array[Double] ) => 5 + (1/(math.pow(x(0),2) + math.pow(x(1),2)))
  
  def global_best_position(pos:Array[Double])={
    if(obj_func(pos) < obj_func(gbest_position)){
      gbest_position=pos
    }
  }  
  
  def init_pbest(p:Particle)={
    p.p_best = p.p_position
  }  
}