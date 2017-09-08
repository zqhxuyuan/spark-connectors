package com.zqh.spark.connectors.inject.cake

import com.zqh.spark.connectors.inject.User

/**
  * Created by zhengqh on 17/9/4.
  */
object UserAppMock extends App with UserServiceComponent with UserDALComponent{

  val userService = new UserService
  val userDAL = new UserDAL // pass mocked UserDAL object for unit testing

  def getUserInfo(id: String): User = {
    val user = userDAL.getUser(id)
    println("UserService: Getting user " + user)
    user
  }

}

//Data access layer
trait UserDALComponent {
  val userDAL: UserDAL

  class UserDAL {
    // a dummy data access layer
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
}

// User service which have Data Access Layer dependency
trait UserServiceComponent { this: UserDALComponent =>

  val userService: UserService

  class UserService {
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
}