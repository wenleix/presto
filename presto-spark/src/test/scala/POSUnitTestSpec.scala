package com.facebook.presto.spark

import org.scalatest._

abstract class POSUnitTestSpec
  extends FlatSpec
    with Matchers
    with OptionValues
    with Inside
    with Inspectors
    with BeforeAndAfterAll
