package edu.sjsu.cmpe282.api.resources;


import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONObject;

import edu.sjsu.cmpe282.domain.User;
import edu.sjsu.cmpe282.dto.UserDao;

@Path("/users")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class UserResource {
	
	private UserDao userdao = new UserDao();
	
	@POST
	@Path("/signup")
	public String signUp(User user) throws ClassNotFoundException {
		
		System.out.print("user created: "+user.getFirstName());
		
		return userdao.addUser(user);
	}

	@POST
	@Path("/signin")
	public String signIn(User user)
	{
		System.out.println("In Resource"+"\t"+user.getEmail()+"\t"+user.getFirstName()+"\t"+user.getLastName()+"\t"+user.getPasswd());
		
		return userdao.checkUser(user);  //boolean
		
		//return "rest operation";
	}
	
	@POST
	@Path("/getCatalog")
	public String getCatalogs()
	{
		
		return userdao.getCatalog();
		
	}
	
	@POST
	@Path("/addCatalog")
	public String addCatalog(String catalogDetails)
	{
		
		return userdao.addCatalog(catalogDetails);
		
	}
	@POST
	@Path("/addProduct")
	public String addProduct(String productDetails)
	{
		
		return userdao.addProduct(productDetails);
		
	}
	
	@POST
	@Path("/getProducts")
	public String getProducts(String categoryID)
	{
		
		
		return userdao.getProducts(categoryID);
	}
	
	@GET
	@Path("/getCategory")
	public String getCategory(String catalogID )
	{
	
		return userdao.getCategory(catalogID);
	}
	
	@POST
	@Path("/viewItemsInCart")
	public String viewItemsInCart(String emailID)
	{
		
		return userdao.viewItemsInCart(emailID);
	}
	@POST
	@Path("/addToCart")
	public String addToCart(String cartDetails)
	{
			
		return userdao.updateCart(cartDetails);
		
	}
	
	@POST
	@Path("/removeFromCart")
	public String removeFromCart(String cartItem)
	{
			
		return userdao.removeFromCart(cartItem);
		
	}
	@POST
	@Path("/addPayment")	
	public String addOrderPayment(String orderDetails)
	{
		
		return userdao.addOrderPayment(orderDetails);
	}
	@POST
	@Path("/orderHistory")
	public String orderHistory(String emailID){
		
		return userdao.orderHistory(emailID);
	} 
	
}
