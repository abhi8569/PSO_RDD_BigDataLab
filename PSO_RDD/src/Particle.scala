import scala.collection.Seq

class Particle(dimension:Int,iter:Int) extends Serializable  {
  var p_id:Int=iter
  var p_position = (Seq.fill(dimension)(math.random)).toArray
  var p_velocity = (Seq.fill(dimension)(math.random)).toArray
  var p_best = (Seq.fill(dimension)(math.random)).toArray
  
  var p_fitness = 0.0
  var pbest_fitness = 0.0
  
  def compute_pfitness() ={
    p_fitness=obj_func(p_position)
  }
  
  def compute_pbest_fitness() ={
    pbest_fitness=obj_func(p_best)
  }
  
  def obj_func(x:Array[Double]):Double = {
      var temp:Double  =0
      for(dim <- 0 to x.length-1 )
      {
        temp =temp + (math.pow(x(dim),2))
      }
      return temp
    }
  
}