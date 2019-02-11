package com.eiconnector.ExcelImportConnector;

public class Test {
	public static void main(String[] args) {
		String name = "file_fscmtopmodelam_finapinvtransactionsam_invoicedistributionpvo-batch606540695-20190114_054710";
		String n = name.substring(name.indexOf('_')+1, name.lastIndexOf('_'));
		System.out.println(n.substring(n.indexOf('_')+1,n.lastIndexOf('_')));
	}
}
