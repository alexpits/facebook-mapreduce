package social;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/result")
public class ResultController {

	@Autowired
	private HBaseService hBase;

	@Value("${min.wordcount}")
	private int minCount;

	@RequestMapping(method = RequestMethod.GET)
	public String getResult(Model model, @RequestParam(value = "id", required = false) String userId)
			throws IOException {
		if (userId == null) {
			model.addAttribute("resultList", hBase.getUsers());
			return "resultList";
		} else {
			model.addAttribute("user", hBase.getUser(userId));
			model.addAttribute("wordcount", hBase.getWords(userId, minCount));
			return "result";
		}
	}

}