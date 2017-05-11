/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.runtime

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{CompositeType, TypeSerializer}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.table.runtime.types.{CRow, CRowSerializer}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Wrap input to rawBytes
  */
class HashWrapper[T](raw: T, serializer: TypeSerializer[T]) extends Serializable {
  val rawBytes = raw match {
    case cRow: CRow =>
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out: DataOutputViewStreamWrapper = new DataOutputViewStreamWrapper(baos)
      serializer.asInstanceOf[CRowSerializer].serialize(cRow, out)
      baos.toByteArray
    case _ =>
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out: DataOutputViewStreamWrapper = new DataOutputViewStreamWrapper(baos)
      serializer.serialize(raw, out)
      baos.toByteArray
  }

  override def hashCode(): Int = util.Arrays.hashCode(rawBytes)

  def canEqual(other: Any) = other.isInstanceOf[HashWrapper[T]]

  override def equals(other: Any) = other match {
    case that: HashWrapper[T] =>
      that.canEqual(this) && util.Arrays.equals(this.rawBytes, that.rawBytes)
    case _ => false
  }
}

/**
  * Connect data for left stream and right stream.
  * only use for innerJoin or leftOuterJoin
  *
  * @param joiner            join function for two records
  * @param leftType
  * @param rightType
  * @param resultType
  * @param defaultRightValue this is a empty record, only use for left outer join
  */
class StreamConnectCoMapFunction(
                                           joiner: RichFlatJoinFunction[Row, Row, Row],
                                           leftType: CompositeType[CRow],
                                           rightType: CompositeType[CRow],
                                           resultType: TypeInformation[CRow],
                                           defaultRightValue: Option[Row]
                                         ) extends
  RichCoFlatMapFunction[CRow, CRow, CRow] with ResultTypeQueryable[CRow] {

  @transient var leftSerializer: TypeSerializer[CRow] = null;
  @transient var rightSerializer: TypeSerializer[CRow] = null;
  @transient var leftStateDescriptor: MapStateDescriptor[HashWrapper[CRow], Int] = null
  @transient var rightStateDescriptor: MapStateDescriptor[HashWrapper[CRow], Int] = null
  @transient var leftState: MapState[HashWrapper[CRow], Int] = null
  @transient var rightState: MapState[HashWrapper[CRow], Int] = null
  @transient var cRowWrapper: CRowWrappingCollector = null

  override def open(parameters: Configuration): Unit = {
    leftSerializer = leftType.createSerializer(getRuntimeContext.getExecutionConfig)
    rightSerializer = rightType.createSerializer(getRuntimeContext.getExecutionConfig)
    leftStateDescriptor = new MapStateDescriptor("left", classOf[HashWrapper[CRow]], classOf[Int])
    rightStateDescriptor = new MapStateDescriptor("right", classOf[HashWrapper[CRow]], classOf[Int])
    leftState = getRuntimeContext.getMapState(leftStateDescriptor)
    rightState = getRuntimeContext.getMapState(rightStateDescriptor)
    cRowWrapper = new CRowWrappingCollector()
    joiner.setRuntimeContext(getRuntimeContext)
    joiner.open(parameters)
  }

  // call flatMap1 if input is from the left of join
  override def flatMap1(value: CRow, out: Collector[CRow]): Unit = {

    cRowWrapper.out = out
    cRowWrapper.setChange(value.change)

    val serializedValue = new HashWrapper[CRow](value, leftSerializer)

    // if serializedValue does not exist in leftState, MapState will return null
    // then scala will cast null to Int which returns 0 to oldValue
    var oldValue = leftState.get(serializedValue)
    // update left stream state

    if (!value.asInstanceOf[CRow].change) {
      oldValue = oldValue - 1
      if (oldValue <= 0) {
        leftState.remove(serializedValue)
      } else {
        leftState.put(serializedValue, oldValue)
      }
      getRuntimeContext.getLongCounter("Left deleted fk").add(1)
    } else {
      oldValue = oldValue + 1
      leftState.put(serializedValue, oldValue)
      getRuntimeContext.getLongCounter("Left added fk").add(1)
    }

    // join right data
    if (null != rightState.keys() && rightState.keys().iterator().hasNext) {
      rightState.keys().asScala.foreach(r => {
        val bais: ByteArrayInputStream = new ByteArrayInputStream(r.rawBytes)
        val in: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(bais)
        val rightValue = rightSerializer.asInstanceOf[CRowSerializer].deserialize(in)
        val times = rightState.get(r)
        for (_ <- 0 until times) {
          joiner.join(value.row, rightValue.row, cRowWrapper)
        }
      })
    } else if (defaultRightValue.isDefined) {
      // defaultRightValue is Some[Row] indicates a LeftOuterJoin
      joiner.join(value.row, defaultRightValue.get, cRowWrapper)
    }

  }

  // call flatMap2 if input is from the right of join
  override def flatMap2(value: CRow, out: Collector[CRow]): Unit = {

    val serializedValue = new HashWrapper[CRow](value, rightSerializer)

    // if serializedValue does not exist in rightState, MapState will return null
    // then scala will cast null to Int which returns 0 to oldValue
    var oldValue = rightState.get(serializedValue)

    // update right stream state
    if (!value.asInstanceOf[CRow].change) {
      oldValue = oldValue - 1
      if (oldValue <= 0) {
        rightState.remove(serializedValue)
      } else {
        rightState.put(serializedValue, oldValue)
      }
      getRuntimeContext.getLongCounter("Right deleted fk").add(1)
    } else {
      oldValue = oldValue + 1
      rightState.put(serializedValue, oldValue)
      getRuntimeContext.getLongCounter("Right added fk").add(1)
    }

    // join left data
    if (null != leftState.keys() && leftState.keys().iterator().hasNext) {
      leftState.keys().asScala.foreach(l => {
        val bais: ByteArrayInputStream = new ByteArrayInputStream(l.rawBytes)
        val in: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(bais)
        val leftValue = leftSerializer.asInstanceOf[CRowSerializer].deserialize(in)
        val times = leftState.get(l)
        for (_ <- 0 until times) {
          joiner.join(leftValue.row, value.row, cRowWrapper)
        }
      })
    }
  }

  override def getProducedType: TypeInformation[CRow] = resultType
}

