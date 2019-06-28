package models

case class Key(deviceId: Int)

case class DeviceMeasurements(date: Int, time: Int, key: Long, value: Float, valueType: String)

