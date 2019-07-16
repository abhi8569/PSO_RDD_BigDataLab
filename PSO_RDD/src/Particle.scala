import scala.collection.Seq

class Particle(dimension:Int,iter:Int) extends Serializable  {
  var p_id:Int=iter
  var p_position = (Seq.fill(dimension)(math.random)).toArray
  var p_velocity = (Seq.fill(dimension)(math.random)).toArray
  var p_best = (Seq.fill(dimension)(math.random)).toArray
  
}