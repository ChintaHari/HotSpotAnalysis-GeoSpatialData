package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectangle_coordinates = queryRectangle.split(",")
	val target_point_coordinates = pointString.split(",")
	val x1: Double = math.min(rectangle_coordinates(0).trim.toDouble, rectangle_coordinates(2).trim.toDouble)
	val y1: Double = math.min(rectangle_coordinates(1).trim.toDouble, rectangle_coordinates(3).trim.toDouble)
	val x2: Double = math.max(rectangle_coordinates(0).trim.toDouble, rectangle_coordinates(2).trim.toDouble)
	val y2: Double = math.max(rectangle_coordinates(1).trim.toDouble, rectangle_coordinates(3).trim.toDouble)

	if ((target_point_coordinates(0).trim.toDouble >= x1) && (target_point_coordinates(0).trim.toDouble <= x2)
		&&
	   (target_point_coordinates(1).trim.toDouble >= y1) && (target_point_coordinates(1).trim.toDouble <= y2)) {
		return true
	}
	return false

}

}
