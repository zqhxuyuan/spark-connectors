package com.zqh.spark.connectors.inject.guice

import com.google.inject.{Guice, Inject}
import com.zqh.spark.connectors.inject.User

/**
  * Created by zhengqh on 17/9/4.
  */
object UserAppGuide {

  def main(args: Array[String]) {
    val injector = Guice.createInjector(new DependencyModule)
    val component = injector.getInstance(classOf[UserService])

    component.createUser(User("12334", "testUser", "test@knoldus.com"))
  }
}

import com.google.inject.{ Inject, Module, Binder, Guice}
import com.google.inject.name.Names

class DependencyModule extends Module {

  def configure(binder: Binder) = {
    // 将UserDALComponent接口的实现类绑定到UserDAL上
    binder.bind(classOf[UserDALComponent]).to(classOf[UserDAL])
  }
}

trait UserDALComponent {

  def getUser(id: String): User
  def create(user: User)
  def delete(user: User)

}

class UserDAL extends UserDALComponent {
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

// User service which have Data Access Layer dependency
class UserService @Inject()(userDAL: UserDALComponent) {

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
