using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Clasification
{
    class Program
    {
        static void Main(string[] args)
        {
            string Connect = "Database=rs2;Data Source=127.0.0.1;User Id=root;Password=1234";
            MySqlConnection myConnection = new MySqlConnection(Connect);

            string queryStringClearTable = @"truncate table ratingsduplicate2;
                                             truncate table userfilmssummfilms;
                                             truncate table userfilmsperesech;
                                             truncate table userfilmsobiedineniehelp;
                                             truncate table clasificationclaster;
                                             truncate table distances1;
                                             truncate table distances2;
                                             truncate table clasificationclaster;
                                             truncate table ksosed;
                                             truncate table recomendation;
                                             delete from ratingsduplicate where userid=1;
                                             delete from userfilmskolvoclaster1 where userid=1;
                                             delete from userfilmskolvo where userid=1;";
            MySqlCommand myCommandClearTable = new MySqlCommand(queryStringClearTable, myConnection);


            
            myConnection.Open();
            myCommandClearTable.ExecuteNonQuery();

            int ClassificationUserId = 2;
            Console.Write("Write pls userId");
            Console.Write("\n");
            ClassificationUserId = int.Parse(Console.ReadLine());
            Console.Write("\n");
            DateTime time1 = DateTime.Now;
            Console.WriteLine(time1);

            //копируем оценки по просмотренным фильмам из ratings в ratinsduplicate и в ratinsduplicate2
            string queryStringZapolnaemTableRatingsduplicate1 = @"insert into ratingsduplicate select * from ratings where userid=" + ClassificationUserId;
            MySqlCommand myCommandZapolnaemTableRatingsduplicate1 = new MySqlCommand(queryStringZapolnaemTableRatingsduplicate1, myConnection);
            myCommandZapolnaemTableRatingsduplicate1.CommandTimeout = 0;
            myCommandZapolnaemTableRatingsduplicate1.ExecuteNonQuery();
            string queryStringZapolnaemTableRatingsduplicate2 = @"insert into ratingsduplicate2 select * from ratings where userid=" + ClassificationUserId;
            MySqlCommand myCommandZapolnaemTableRatingsduplicate2 = new MySqlCommand(queryStringZapolnaemTableRatingsduplicate2, myConnection);
            myCommandZapolnaemTableRatingsduplicate2.CommandTimeout = 0;
            myCommandZapolnaemTableRatingsduplicate2.ExecuteNonQuery();


            //определяем Кол-во фильмов у нового пользователя
            string queryStringKolvoFilms = @"insert into userfilmskolvoclaster1
                                                select userId, count(*)
                                                from ratingsduplicate
                                                where userid=" + ClassificationUserId;

            MySqlCommand myCommandKolvoFilms = new MySqlCommand(queryStringKolvoFilms, myConnection);
            myCommandKolvoFilms.CommandTimeout = 0;
            myCommandKolvoFilms.ExecuteNonQuery();

            
            //заполняем таблицу userfilmsperesech и userfilmssummfilms
            //userfilmsperesech             
            string queryStringUserFilmsPeresechUserId = @"insert into userfilmsPeresech
                                                                    select t.User1Id, t.User2Id, count(*) as p1
                                                                    from (select a1.userId as User1Id, a2.userId as User2Id, a1.filmId as filmId
                                                                          from ratingsduplicate as a1 inner join ratingsduplicate as a2 on a1.filmId=a2.filmId 
                                                                          where a1.userId=" + ClassificationUserId + @" and a1.userId < a2.userId) as t
                                                                    group by t.User1Id, t.User2Id";
            MySqlCommand myCommandUserFilmsPeresechUserId = new MySqlCommand(queryStringUserFilmsPeresechUserId, myConnection);
            myCommandUserFilmsPeresechUserId.CommandTimeout = 0;
            myCommandUserFilmsPeresechUserId.ExecuteNonQuery();

            
            //userfilmssummfilms
            //Кол-во фильмов пары пользователей (рассматриваются всевозможные пары пользователей)
            string queryStringObsieFilms = @"insert into userfilmssummfilms
                                                    select a1.userId as user1Id, a2.userId as user2Id, (a1.kolvo + a2.kolvo) as p2
                                                    from userfilmskolvoclaster1 as a1, userfilmskolvoclaster1 as a2 
                                                    where a1.userId=" + ClassificationUserId + @" and  a1.userId < a2.userId";
            MySqlCommand myCommandObsieFilms = new MySqlCommand(queryStringObsieFilms, myConnection);
            myCommandObsieFilms.CommandTimeout = 0;
            myCommandObsieFilms.ExecuteNonQuery();

            //определение объединения фильмов
            string queryStringuserfilmsobiedineniehelp = @"insert into userfilmsobiedineniehelp select a1.user1Id, a1.user2Id, (a2.p2 - a1.p1) as p3
                                                                                            from userfilmsperesech as a1 inner join userfilmssummfilms as a2 
                                                                                            on  a1.user2Id=a2.user2Id and a1.user1Id=a2.user1Id and a1.user1id=" + ClassificationUserId;
            MySqlCommand myCommanduserfilmsobiedineniehelp = new MySqlCommand(queryStringuserfilmsobiedineniehelp, myConnection);
            myCommanduserfilmsobiedineniehelp.CommandTimeout = 0;
            myCommanduserfilmsobiedineniehelp.ExecuteNonQuery();

            //определение коэффициентов Танимото
            string queryStringKTanimoto = @"insert into distances1 select a2.user1id, a2.user2id, a1.p1 / a2.p3 
                                                                from userfilmsperesech as a1, userfilmsobiedineniehelp as a2
                                                                where a1.user2Id=a2.user2Id and a1.user1Id=a2.user1Id and a1.user1id=" + ClassificationUserId;
            MySqlCommand myCommandKTanimoto = new MySqlCommand(queryStringKTanimoto, myConnection);
            myCommandKTanimoto.CommandTimeout = 0;
            myCommandKTanimoto.ExecuteNonQuery();





            //определение пользователей с max коэффициентом Танимото из таблицы distances1
            string queryStringMaxKTanimoto = @"select *  
                                               from distances1 as t 
                                               where t.distance1= (select max(distance1) as max 
                                                                   from distances1)";

            MySqlCommand myCommandMaxKTanimoto = new MySqlCommand(queryStringMaxKTanimoto, myConnection);
            myCommandMaxKTanimoto.CommandTimeout = 0;
            MySqlDataReader MaxKTanimoto = myCommandMaxKTanimoto.ExecuteReader();

            MaxKTanimoto.Read();
            string userIdTanimoto = MaxKTanimoto.GetString(1);
            double KTanimoto = MaxKTanimoto.GetDouble(2);
            MaxKTanimoto.Close();



            string queryStringED = @"insert into ratingsduplicate2 select * from ratingsforclaster where userid in (select userid from clasters1 where claster1id=" + userIdTanimoto + @") or userid = " + userIdTanimoto;
            MySqlCommand myCommandED = new MySqlCommand(queryStringED, myConnection);
            myCommandED.CommandTimeout = 0;
            myCommandED.ExecuteNonQuery();

            //определяем расстояние между всевозможными парами пользователей (заносим информацию в таблицу distances2)
            string queryStringEuclideanDistance = @"insert into distances2 
                                                            select t.user1Id, t.user2Id, (1/(1+(sqrt(sum(t.raznost2))))) as distance2
                                                            from (select a1.userId as User1Id, a2.userId as User2Id, (a1.rating - a2.rating)*(a1.rating - a2.rating) as raznost2
                                                                  from ratingsduplicate2 as a1 inner join ratingsduplicate2 as a2 on a1.filmId=a2.filmId and a1.userid= " + ClassificationUserId + @" and a1.userId<a2.userId)as t
                                                            group by t.user1Id, t.user2Id";
            MySqlCommand myCommandEuclideanDistance = new MySqlCommand(queryStringEuclideanDistance, myConnection);
            myCommandEuclideanDistance.CommandTimeout = 0;
            myCommandEuclideanDistance.ExecuteNonQuery();



            // определяем значение K для метода классификации к ближайших соседей.
            string queryStringK1 = @"select count(*) as a  from clasters2 where userid in (select userid from clasters1 where claster1id=" + userIdTanimoto + @") or userid=" + userIdTanimoto + @" or 
					claster2id in (select userid from clasters1 where claster1id=" + userIdTanimoto + @") or claster2id = " + userIdTanimoto + @"
					group by claster2id order by a limit 1";
            

            MySqlCommand myCommandK1 = new MySqlCommand(queryStringK1, myConnection);
            myCommandK1.CommandTimeout = 0;
            MySqlDataReader myCommandK = myCommandK1.ExecuteReader();

            myCommandK.Read();
            int K = myCommandK.GetInt32(0);
            myCommandK.Close();
            K = K + 1;

            // определение K ближайший соседей
            string queryStringUserKSosed = @"insert into ksosed select user2Id from distances2 order by distance2 desc limit " + K;
            MySqlCommand myCommandUserKSosed = new MySqlCommand(queryStringUserKSosed, myConnection);
            myCommandUserKSosed.CommandTimeout = 0;
            myCommandUserKSosed.ExecuteNonQuery();

            //классификация нового пользователя к кластеру
            string queryStringclasificationclaster = @" insert into clasificationclaster 
                                                select claster2id, " + ClassificationUserId + @" 
                                                from (select claster2id, count(*) as r 
                                                      from clasters2 where userid in (select * from ksosed) or claster2id in (select * from ksosed) 
                                                      group by claster2id order by r desc limit 1)as t";
            MySqlCommand myCommandclasificationclaster = new MySqlCommand(queryStringclasificationclaster, myConnection);
            myCommandclasificationclaster.CommandTimeout = 0;
            myCommandclasificationclaster.ExecuteNonQuery();

            //прогнозирование оценки пользователя по еще не просмотренным фильмам + выдача рекомендации
            string queryStringPrognozOcenki = @"insert into recomendation select user1Id,filmid, sumR/sumP as rating 
                                                from (select *, sum(R) as sumR, sum(distance2) as sumP 
                                                      from (select *,a1.distance2*a2.rating as R 
                                                            from (select * 
                                                                  from distances2 where user2id in (select userid 
                                                                                                    from clasters2 
						                                                                            where claster2id=(select claster2id from clasificationclaster where userid=" + ClassificationUserId + @"))
							                                                                        or user2id=(select claster2id from clasificationclaster where userid=" + ClassificationUserId + @")) as a1 inner join 
							 				                      (select * from ratingsforclaster where filmid not in (select filmid from ratings where userid=" + ClassificationUserId + @")) as a2
                                                                  on a1.user2id=a2.userid) as d
							 				          group by filmid) as f";
            MySqlCommand myCommandPrognozOcenki = new MySqlCommand(queryStringPrognozOcenki, myConnection);
            myCommandPrognozOcenki.CommandTimeout = 0;
            myCommandPrognozOcenki.ExecuteNonQuery();

            Console.WriteLine("Recomendation:");
            Console.Write("\n");
            DateTime time2 = DateTime.Now;
            Console.WriteLine(time2);

            string queryStringCountRec = @"select count(*) from recomendation";
            MySqlCommand myCommandCountRec = new MySqlCommand(queryStringCountRec, myConnection);
            myCommandCountRec.CommandTimeout = 0;
            string stringCountRec = myCommandCountRec.ExecuteScalar().ToString();
            int CountRec = Convert.ToInt32(stringCountRec);

            string[] RecFilmName = new string[CountRec];
            string[] RecRating = new string[CountRec];


            string queryStringRec = @"select a2.`Название`,a1.rating from recomendation as a1 inner join films as a2 on a1.filmid=a2.id order by rating desc";
            MySqlCommand myCommandRec = new MySqlCommand(queryStringRec, myConnection);
            myCommandRec.CommandTimeout = 0;
            MySqlDataReader RecWrite = myCommandRec.ExecuteReader();
            int z = 0;
            while (RecWrite.Read())
            {
                RecFilmName[z] = RecWrite.GetString(0);
                RecRating[z] = RecWrite.GetString(1);
                Console.WriteLine(RecFilmName[z]);
                Console.WriteLine(RecRating[z]);
                Console.Write("\n");
                z++;
            }
            RecWrite.Close();
            Console.ReadLine();






            myConnection.Close();
        }
    }
}
