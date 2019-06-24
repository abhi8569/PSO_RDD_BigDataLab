import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PSO_RDD {
  //hallo
  var dimension = 2
  var no_of_particles = 50
  var gbest_position=Array.fill(dimension)(math.random)
    
  def main(args:Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("PSO_RDD").setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    var swarm = ArrayBuffer[Particle]()
    for(i <- 0 to no_of_particles-1){
      swarm += new Particle(dimension) with Serializable
    }
    var swarm_rdd = sc.parallelize(swarm)

    swarm_rdd.foreach(f => global_best_position(f.p_position))
    
    var temp_rdd: RDD[Particle]= sc.emptyRDD[Particle]
    temp_rdd=swarm_rdd
    var updated_velocity_swarm: RDD[Particle]= sc.emptyRDD[Particle]
    
    for(iteration <- 0 to 100){
      updated_velocity_swarm = temp_rdd.map{x => update_particle(x)}
      updated_velocity_swarm.count()  //dummy action to trigger map
      temp_rdd = updated_velocity_swarm
      println("Value : ",obj_func(gbest_position))
    }
    
    def update_particle(p:Particle):Particle={
      
      var parti = new Particle(dimension)
      parti = p
      
      for(i <- 0 to dimension-1){
        var first_part= math.random*(parti.p_best(i)-parti.p_position(i))
        var second_pat =  2*math.random*(gbest_position(i)-parti.p_position(i))
        parti.p_velocity(i) = 0.5*parti.p_velocity(i) + first_part + second_pat
      }
      for(i <- 0 to dimension-1){
        parti.p_position(i) += parti.p_velocity(i)
      }
      
      if(obj_func(parti.p_position) < obj_func(parti.p_best)){
        parti.p_best = parti.p_position
      }
      
      if(obj_func(parti.p_best) < obj_func(gbest_position)){
        gbest_position=parti.p_position
      }
      
      return parti
    }
    sc.stop()
  }
  
  val obj_func = (x:Array[Double] ) => 1 + (1/(math.pow(x(0),2) + math.pow(x(1),2)))
  
  def global_best_position(pos:Array[Double])={
    if(obj_func(pos) < obj_func(gbest_position)){
      gbest_position=pos
    }
  }  
}