package com.zqh.spark.connectors.inject.constructor

import com.zqh.spark.connectors.inject.User

trait UserDALComponent {

  def getUser(id: String): User
  def create(user: User)
  def delete(user: User)

}

class UserDALGood extends UserDALComponent {

  // a dummy data access layer that is not persisting anything
  def getUser(id: String): User = {
    val user = User("12334", "testUser", "test@knoldus.com")
    println("UserDAL: Getting user " + user)
    user
  }
  def create(user: User) = {
    println("UserDAL: creating user: " + user)
  }
  def delete(user: User) = {
    println("UserDAL: deleting user: " + user)
  }

}

// UserService depend on UserDALComponent(abstraction) not implemention.
class UserServiceGood(userDAL: UserDALComponent) {

  def getUserInfo(id: String): User = {
    val user = userDAL.getUser(id)
    println("UserService: Getting user " + user)
    user
  }

  def createUser(user: User) = {
    userDAL.create(user)
    println("UserService: creating user: " + user)
  }

  def deleteUser(user: User) = {
    userDAL.delete(user)
    println("UserService: deleting user: " + user)
  }

}