import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PSO_RDD {
  
  var dimension = 20
  var no_of_particles = 500
  var no_of_iteration_External = 1000
  var no_of_iteration_Internal = 1
  var g_best=Array.fill(dimension)(math.random)
  var gbest_fitness = 0.0   
  def main(args:Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("2W4C3G :: "+dimension+"-"+no_of_particles+"-"+no_of_iteration_External+"-"+no_of_iteration_Internal).setMaster("spark://abhi8569:7077").set("spark.eventLog.enabled","true")
              .set("spark.eventLog.dir","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
              //.set("spark.history.fs.logDirectory","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
    val sc = new SparkContext(conf)
    
    var swarm = ArrayBuffer[Particle]()
    for(i <- 0 to no_of_particles-1){
      swarm += new Particle(dimension,i) with Serializable 
    }
    var swarm_rdd = sc.parallelize(swarm,8)

    swarm_rdd.foreach(f => init_pbest(f))
    swarm_rdd.foreach(f => f.compute_pbest_fitness())
    swarm_rdd.foreach(f => f.compute_pfitness())
    swarm_rdd.foreach(f => global_best_position(f))
    
    var temp_rdd: RDD[Particle]= sc.emptyRDD[Particle]
    temp_rdd=swarm_rdd
    var updated_velocity_swarm: RDD[Particle]= sc.emptyRDD[Particle]
    
    var bCast = sc.broadcast(g_best)
    
    println(obj_func(g_best))
    for(iteration <- 0 to no_of_iteration_External){
      updated_velocity_swarm = temp_rdd.map{x => update_particle(x)}
      temp_rdd = updated_velocity_swarm
      temp_rdd.collect().foreach(f => global_best_position(f))
      println(obj_func(g_best))
      bCast.unpersist(blocking=true)
      bCast = sc.broadcast(g_best)
    }
    
    def update_particle(p:Particle):Particle={
      g_best = bCast.value
     for(iter <- 0 to no_of_iteration_Internal){
      for(i <- 0 to dimension-1){
        var toward_pbest= math.random*(p.p_best(i)-p.p_position(i))
        var toward_gbest =  2*math.random*(g_best(i)-p.p_position(i))
        p.p_velocity(i) = 0.5*p.p_velocity(i) + toward_pbest + toward_gbest
      }
      for(i <- 0 to dimension-1){
        p.p_position(i) = p.p_position(i) + p.p_velocity(i)
      }
      
      p.compute_pfitness()
      
      if(p.p_fitness < p.pbest_fitness){
        p.p_best = p.p_position
      }
      
      p.compute_pbest_fitness()
      
      if(p.pbest_fitness < obj_func(g_best)){
        g_best=p.p_position
      }
      //println("Iteration Number : "+iter+" || Particle Number :  "+p.p_id+" || Pbest : "+obj_func(p.p_best)+" || Gbest : "+obj_func(gbest_position))
      }
      return p
    }
    sc.stop()
  }
  
  //val obj_func = (x:Array[Double] ) => 1 + (1/(math.pow(x(0),2) + math.pow(x(1),2)))
  
  //val obj_func = (x:Array[Double] ) => math.pow(x(0),2) + math.pow(x(1),2)
  
  def obj_func(x:Array[Double]):Double = {
      var temp:Double  =0
      for(dim <- 0 to x.length-1 )
      {
        temp =temp + (math.pow(x(dim),2))
      }
      return temp
    }
  
  
  def global_best_position(p:Particle)={
    if(p.pbest_fitness < obj_func(g_best)){
      g_best=p.p_best
    }
  }  
  
  def init_pbest(p:Particle)={
    p.p_best = p.p_position
  }  
}