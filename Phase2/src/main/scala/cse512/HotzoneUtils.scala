package cse512
import scala.math._

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // Parse Point Query and Convert to Double
    val data = pointString.split(",")
    // Store x and y coordinates of point
    val data_x = data(0).toDouble
    val data_y = data(1).toDouble
    // Parse queryRectangle and Convert to Double
    val rectangle = queryRectangle.split(",")
    // Store x and y coordinates of rectangle
    val rect_x1 = min(rectangle(0).toDouble, rectangle(2).toDouble)
    val rect_y1 = min(rectangle(1).toDouble, rectangle(3).toDouble)
    val rect_x2 = max(rectangle(0).toDouble, rectangle(2).toDouble)
    val rect_y2 = max(rectangle(1).toDouble, rectangle(3).toDouble)
    // Check if point is within the rectangle
    return (data_x >= rect_x1 && data_x <= rect_x2 && data_y >= rect_y1 && data_y <= rect_y2)
  }
}
