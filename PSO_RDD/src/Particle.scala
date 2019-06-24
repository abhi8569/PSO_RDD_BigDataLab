import scala.collection.Seq

class Particle(dimension:Int) extends Serializable  {
  
  val start = -100
  val end = 200
  
  var p_position = (Seq.fill(dimension)(math.random*end+start)).toArray
  var p_velocity = (Seq.fill(dimension)(0.0)).toArray
  var p_best = p_position
  
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