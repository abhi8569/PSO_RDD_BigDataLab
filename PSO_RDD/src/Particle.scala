import scala.collection.Seq

class Particle(dimension:Int) extends Serializable  {
  
  val start = -8
  val end = 16
  
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
}