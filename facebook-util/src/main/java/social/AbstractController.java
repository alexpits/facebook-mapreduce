package social;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

public abstract class AbstractController {
	
	@RequestMapping(method = RequestMethod.GET)
	public String loginFacebook() {
		return "redirect:/connect/facebook";
	}

}
