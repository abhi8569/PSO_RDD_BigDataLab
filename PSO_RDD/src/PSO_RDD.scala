import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

class SetAccumulator[T](var value: Set[T]) extends AccumulatorV2[T, Set[T]] {
  def this() = this(Set.empty[T])
  override def isZero: Boolean = value.isEmpty
  override def copy(): AccumulatorV2[T, Set[T]] = new SetAccumulator[T](value)
  override def reset(): Unit = value = Set.empty[T]
  override def add(v: T): Unit = value = value + v
  override def merge(other: AccumulatorV2[T, Set[T]]): Unit = value = value ++ other.value
}

object PSO_RDD {
  
  var dimension = 2
  var no_of_particles =4
  var no_of_partition = 4
  var no_of_iteration = 10
  var gbest_position=Array.fill(dimension)(math.random)
    
  def main(args:Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("PSO_Split_Functionality").setMaster("spark://abhi8569:7077").set("spark.eventLog.enabled","true")
              .set("spark.eventLog.dir","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
              .set("spark.history.fs.logDirectory","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
    val sc = new SparkContext(conf)
    
    var swarm = ArrayBuffer[Particle]()
    for(i <- 0 to no_of_particles-1){
      swarm += new Particle(dimension,i) with Serializable
    }
    var newSwarm = swarm.map(x => init_pbest(x))
    newSwarm.map(f => global_best_position(f.p_best))
    var swarm_rdd = sc.parallelize(newSwarm,no_of_partition)
    
    val acc = new SetAccumulator[Array[Double]]()
    sc.register(acc)
    acc.add(gbest_position)
    println("------------------")
      acc.value.foreach(f => f.foreach(print))
      println("------------------")
    def update_velocity(p:Particle): Particle= {
      for(i <- 0 to no_of_iteration){
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
      
      /*if(obj_func(p.p_best) < obj_func(gbest_position)){
          gbest_position=p.p_best
        }*/
      //println("Iteration Number : "+it+" || Particle Number :  "+p.p_id+" || Pbest : "+obj_func(p.p_best)+" || Gbest : "+obj_func(gbest_position))
      }
      
      
      acc.add(gbest_position)
      return p
    }
    
    for(i <- 0 to no_of_iteration/2){
      var updated_swarm = swarm_rdd.map(x => update_velocity(x))
      //swarm_rdd=updated_swarm
      updated_swarm.count()
      //swarm_rdd.foreach(p => println("Iteration Number : "+i+" || Particle Number :  "+p.p_id+" || Pbest : "+p.obj_func(p.p_best)+" || Gbest : "+p.obj_func(gbest_position)))
    }
    
    swarm_rdd.count()
   println("------------------")
      acc.value.foreach(f => println(obj_func(f)))
      println("------------------")
    sc.stop()
  }
  
  def obj_func(x:Array[Double]):Double = {
      var temp:Double  =0
      for(dim <- 0 to x.length-1 )
      {
        temp =temp + (math.pow(x(dim),2))
      }
      return temp
    }
  
  def global_best_position(pos:Array[Double])={
    if(obj_func(pos) < obj_func(gbest_position)){
      gbest_position=pos
      //println("New Gbest : "+obj_func(gbest_position))
    }
    //println("Waste gbest : "+obj_func(pos))
  }  
  
  def init_pbest(p:Particle):Particle={
    p.p_best = p.p_position
    return p
  }  
}