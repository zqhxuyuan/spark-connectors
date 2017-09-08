package com.zqh.spark.connectors.inject.constructor

import com.zqh.spark.connectors.inject.User

/**
  * Created by zhengqh on 17/9/4.
  * https://blog.knoldus.com/2014/07/04/dependency-injection-scala/
  */
class UserDALBad {

  /* dummy data access layer */
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

class UserService(userDAL: UserDALBad) {

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

object UserAppBad {
  def main(args: Array[String]) {
    val userDAL = new UserDALBad
    val userService = new UserService(userDAL)
    userService.createUser(User("12334", "testUser", "test@knoldus.com"))
  }
}
