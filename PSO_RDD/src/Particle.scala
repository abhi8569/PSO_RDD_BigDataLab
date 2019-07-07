import scala.collection.Seq

class Particle(dimension:Int, id:Int) extends Serializable  {
  
  var p_id=id
  
  var p_position = (Seq.fill(dimension)(math.random)).toArray
  var p_velocity = (Seq.fill(dimension)(math.random)).toArray
  var p_best = (Seq.fill(dimension)(math.random)).toArray
  
  def print_position = for(i <- 0 to dimension-1){
      print(p_position(i)," ")
    }
  
  def print_velocity = for(i <- 0 to dimension-1){
      println(p_velocity(i)," ")
    }
  
  def print_pbest = for(i <- 0 to dimension-1){
      print(p_best(i)," ")
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