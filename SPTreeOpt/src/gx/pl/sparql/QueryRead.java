package gx.pl.sparql;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Node;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.javatuples.Triplet;

public class QueryRead {
    
    private Query query = null;
    
    private Set<String> variable = new HashSet();
	    
    private Set<Triplet<String, String, String>> bgp = null;
	       
    public QueryRead(String queryPath){
	
	query = QueryFactory.read(queryPath);
	
	Op op = Algebra.compile(query);
	
	MyOpVisitorBase myOpVisitorBase = new MyOpVisitorBase();
	
	myOpVisitorBase.myOpVisitorWalker(op);
	
	final List<Triple> triples = myOpVisitorBase.getBGP();
	
	for (final Triple triple : triples) {
	    final Node subjectNode = triple.getSubject();
	    final Node predicateNode = triple.getPredicate();
	    final Node objectNode = triple.getObject();

	    final String subject = subjectNode.toString();
	    final String object = objectNode.toString();
	    final String predicate = predicateNode.toString();
	    
	    if (bgp == null) {
		bgp = new HashSet<Triplet<String, String, String>>();
	    }
	    
	    Triplet<String, String, String> triplet = Triplet.with(subject, predicate, object);
	    
	    bgp.add(triplet);
	    
	}
	
    }
	    
    public Set<String> getVarible(){
	
	System.out.println("-----------------start print variables in query----------------------");
	List<Var> var = query.getValuesVariables();
	for(int i=0; i < var.size(); i++){
	    variable.add(var.get(i).toString());
	    System.out.print(var.get(i).toString() + " ");
	}
	System.out.println("-----------------end print variables in query----------------------");
	return variable;
    }
	    
    public Set<Triplet<String, String, String>> getBGP() {
		
	return bgp;
    }
    
}
