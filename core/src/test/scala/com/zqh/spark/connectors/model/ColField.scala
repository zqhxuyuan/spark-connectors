package com.zqh.spark.connectors.model

import scala.beans.BeanProperty

class ColField {

  @BeanProperty
  var fromField: String = _

  @BeanProperty
  var fromType: String = _

  @BeanProperty
  var toField: String = _

  @BeanProperty
  var toType: String = _
}
