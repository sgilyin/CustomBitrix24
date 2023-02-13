package ru.bitel.bgbilling.scripts.services.custom.bitrix24;

import ru.bitel.bgbilling.kernel.script.server.dev.GlobalScriptBase;
import org.apache.log4j.Logger;
import ru.bitel.bgbilling.server.util.Setup;
import ru.bitel.common.sql.ConnectionSet;
import java.sql.Connection;
import java.net.*;
import java.nio.charset.*;
import java.io.*;
import org.json.JSONObject;
import bitel.billing.server.contract.bean.ContractParameterManager;
import java.text.*;
import java.time.ZoneId;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

public class CustomBitrix24
	extends GlobalScriptBase
{
	private static final Logger logger = Logger.getLogger( CustomBitrix24.class );

	@Override
	public void execute( final Setup setup, final ConnectionSet connectionSet) throws Exception {
		logger.info( "begin" );
		Connection con = connectionSet.getConnection();
		for (int i = 0; i < 5; i++) {
			String sqlQuery = getSqlQuery(i);
			String type = getType(i);
			try (PreparedStatement ps = con.prepareStatement(sqlQuery);
			ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int cid = rs.getInt("cid");
					logger.info(type + ": " + cid);
					print(type + ": " + cid);
					taskAdd(type, cid, setup, con);
				}
			}
		}
		logger.info( "end" );
	}

	public String getType (Integer step) {
		String type = "";
		switch (step) {
			case 0:
				type = "pon.ena";
				break;
			case 1:
				type = "eth.ena";
				break;
			case 2:
				type = "ctv.ena";
				break;
			case 3:
				type = "ctv.not";
				break;
			case 4:
				type = "ctv.dis";
				break;
		}
		return type;
	}

	public String getSqlQuery (Integer step) {
		Integer pid1 = 0;
		Integer pid6 = 0;
		String gr = "";
		String sqlQueryTemplate = "SELECT t_c.id cid\n" +
			"FROM contract t_c\n" +
			"LEFT JOIN contract_parameter_type_1 t_cpt1 ON t_cpt1.cid=t_c.id AND t_cpt1.pid=%d\n" +
			"LEFT JOIN contract_parameter_type_6 t_cpt6 ON t_cpt6.cid=t_c.id AND t_cpt6.pid=%d\n" +
			"WHERE t_c.fc = 0 AND t_cpt1.val IS NULL AND NOT t_cpt6.val IS NULL AND t_cpt6.val >= CURRENT_DATE()%s";

		switch (step) {
			case 0:
				pid1 = 43;
				pid6 = 41;
				gr = " AND t_c.gr&(1<<39) > 0";
				break;
			case 1:
				pid1 = 43;
				pid6 = 41;
				break;
			case 2:
				pid1 = 44;
				pid6 = 40;
				break;
			case 3:
				pid1 = 47;
				pid6 = 57;
				break;
			case 4:
				pid1 = 45;
				pid6 = 58;
				break;
		}
		return String.format(sqlQueryTemplate, pid1, pid6, gr);
	}

	public void taskAdd(String type, Integer cid, Setup setup, Connection con) {
		String title = "";
		LocalDateTime sdp = LocalDateTime.now();
		LocalDateTime edp = LocalDateTime.now();
		LocalDateTime dln = LocalDateTime.now();
		String rid = setup.get("bitrix24." + type + ".rid");
		String acs = setup.get("bitrix24." + type + ".acs");
		String ads = setup.get("bitrix24." + type + ".ads");
		String tgs = setup.get("bitrix24." + type + ".tgs");
		String gid = setup.get("bitrix24." + type + ".gid");
		String acd = setup.get("bitrix24." + type + ".acd");
		Integer dpid = Integer.valueOf(setup.get("bitrix24." + type + ".pid.date"));
		Integer tpid = Integer.valueOf(setup.get("bitrix24." + type + ".pid.task"));
		ContractParameterManager cpm = new ContractParameterManager(con);
		String address = cpm.getTextlikeParam(cid,12).replaceAll("\\d{0,6}, г. Кумертау, ", "");
		String subscriber = cpm.getTextlikeParam(cid,1);
		String phone = cpm.getTextlikeParam(cid,2);
		try {
			LocalDateTime date = LocalDateTime.ofInstant(cpm.getDateParam(cid,dpid).toInstant(), ZoneId.systemDefault());
			sdp = date.plusHours(6);
			edp = date.plusHours(15);
			dln = date.plusHours(16);
		} catch (Exception e) {
			//The handling for the code
		}

		String dcpTmp = "ФИО: %s\\nТелефон: <a href='tel:+%s'>%s</a>\\nДоговор в биллинге: <a href='https://fialka.tv/tech/?cid=%d'>%d</a>";
		String dcp = String.format(dcpTmp, subscriber, phone, phone, cid, cid);
		switch (type) {
			case "eth.ena":
				title = "Eth | Подключение | " + address;
				break;
			case "pon.ena":
				title = "PON | Подключение | " + address;
				break;
			case "ctv.ena":
				title = "CTV | Подключение | " + address;
				break;
			case "ctv.dis":
				title = "CTV | Отключение | " + address;
				break;
			case "ctv.not":
				title = "CTV | Предупреждение | " + address;
				break;
		}
		String jsonTemplate = "{\"fields\":{\"TITLE\":\"%s\",\"DESCRIPTION\": \"%s\",\"GROUP_ID\":%s,\"CREATED_BY\":1,\"RESPONSIBLE_ID\":%s,\"ACCOMPLICES\":[%s],\"AUDITORS\":[%s],\"TAGS\":[%s],\"DEADLINE\":\"%s\",\"START_DATE_PLAN\":\"%s\",\"END_DATE_PLAN\":\"%s\",\"ALLOW_CHANGE_DEADLINE\":\"%s\"}}";
		String jsonRequest = String.format(jsonTemplate, title, dcp, gid, rid, acs, ads, tgs, dln, sdp, edp, acd);
		//System.out.println(jsonRequest);
		JSONObject jsonResponse = bx24Exec("tasks.task.add", jsonRequest, setup);
		//System.out.println(jsonResponse);
		JSONObject result = (JSONObject) jsonResponse.get("result");
		JSONObject task = (JSONObject) result.get("task");
		try {
			cpm.updateStringParam(cid, tpid, String.valueOf(task.get("id")), 0);
		} catch (Exception e){

		}

	}

	public JSONObject bx24Exec(String method, String jsonRequest, Setup setup) {
		JSONObject jsonResponse;
		try{
			String bx24Url = setup.get("bitrix24.url");
			String bx24Secret = setup.get("bitrix24.secret");
        	URL url = new URL(bx24Url + "/rest/1/" + bx24Secret + "/" + method + ".json");
        	HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        	connection.setRequestMethod("POST");
        	connection.setDoOutput(true);
        	connection.setRequestProperty("Content-Type","application/json");
        	connection.setRequestProperty("Accept", "application/json");
        	byte[] out = jsonRequest.getBytes(StandardCharsets.UTF_8);
        	OutputStream stream = connection.getOutputStream();
        	stream.write(out);
        	BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        	String inputLine;
        	StringBuffer content = new StringBuffer();
        	while ((inputLine = in.readLine()) != null) {
        	    content.append(inputLine);
        	}
        	in.close();
        	connection.disconnect();
        	//System.out.println(content);
        	jsonResponse = new JSONObject(content.toString());
        	return jsonResponse;
    	} catch (Exception e){
        	return new JSONObject (e.getMessage().toString());
    	}
	}
}