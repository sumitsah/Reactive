import { HttpClient } from '@angular/common/http';
import { inject, Inject, Injectable } from '@angular/core';
import { BehaviorSubject, combineLatest, combineLatestWith, EMPTY, forkJoin, from, interval, mergeMap, Observable, of, shareReplay, startWith, Subject, Subscription, switchMap, tap, withLatestFrom } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class UserService {

  constructor() { }

  private http = inject(HttpClient);
  // private refresh$ = new Subject();
  refresh$ = new Subject<void>();  // Ideally it should be private

  trigger$ = new BehaviorSubject('T1');
  data$ = new BehaviorSubject('D1');

  source$ = interval(5000);

  // following are the use cases for shareReplay
  //  HeaderComponent
  // SidebarComponent
  // ProfileComponent
  // SettingsComponent
  // DashboardComponent 
  user$: Observable<Object> = this.refresh$.pipe(
    startWith(null),
    switchMap(() => this.http.get('https://jsonplaceholder.typicode.com/users/1')),
    shareReplay({ bufferSize: 1, refCount: true })
  )

  // getUsers() {
  //   return this.http.get('https://jsonplaceholder.typicode.com/users')
  // }

  getUserByShareReplay(id: number) {
    // this.user$ = this.refresh$.pipe(
    //   startWith(null),
    //   switchMap(() => this.http.get('https://jsonplaceholder.typicode.com/users')),
    //   shareReplay({ bufferSize: 1, refCount: true })
    // )
    // return this.user$;
  }

  // This is why switchMap is perfect for:
  //   search inputs
  // autocomplete
  // live refresh
  // typeahead

  forkJoinNormal() {
    // return forkJoin([
    //   of(1),
    //   of(2)
    // ])

    // return forkJoin([
    //   of(1, 2, 3),
    //   of('A', 'B')
    // ])
    // return forkJoin([
    //   this.http.get('https://jsonplaceholder.typicode.com/users'),
    //   this.http.get('https://jsonplaceholder.typicode.com/users/1')
    // ])
    // return forkJoin(
    //   {
    //     users: this.http.get('https://jsonplaceholder.typicode.com/users'),
    //     user: this.http.get('https://jsonplaceholder.typicode.com/users/1')
    //   }
    // )

    // return forkJoin([
    //   of([1, 2]),
    //   of([3, 4])
    // ])

    // return forkJoin([
    //   of(1, 2),
    //   of(3, 4)
    // ])

    return forkJoin([
      of([1, 2], [5, 6]),
      of([3, 4], [7, 8])
    ])
  }

  forkJoinEmpty() {
    // EMPTY Observable that completes immediately without emitting anything
    return forkJoin([
      EMPTY,
      of(2)
    ])

    // return forkJoin([
    //   of(null),
    //   of(1)
    // ])
  }

  getFromCombineLatest() {
    return combineLatest([
      of(1, 2, 3),
      of('A', 'B'),
      of('X', 'Y')
    ])

    /* Order of execution for combineLatest 
     of() is synchronous so 
     
     1 -> 2 -> 3 
     A -> B
     X                  as soon as X gets emitted from 3rd observables it has following
 
     obs1 -> 3  (Last emitted value)
     obs2 -> B
     obs3 -> X
 
     [3, B, X]
 
     Now Y has been emitted from 3rd observables
     1 -> 2 -> 3 
     A -> B
     X -> Y
 
     obs1 -> 3  (Last emitted value)
     obs2 -> B  (Last emitted value)
     obs3 -> Y  (Last emitted value)
 
     [3, B, Y]
 
     So the final output will be 
     [3, B, X]
     [3, B, Y]
 
  */
    // return forkJoin([
    //   of(1, 2, 3),
    //   of('A', 'B')
    // ])
  }

  combineLatestWithBehaviour() {
    return combineLatest([
      of(1, 2, 3),
      new BehaviorSubject(100)
    ])
  }

  combineLatestWithBehaviourSwitchmap() {
    return this.trigger$
      .pipe(
        switchMap(() => combineLatest([this.data$, of('X', 'Y')]))
      )
  }

  combineLatestWithExample() {
    return of(1, 2, 3)
      .pipe(
        combineLatestWith(of('A', 'B'))
      )
  }

  withLatestFromExample() {
    return this.source$.pipe(
      withLatestFrom(interval(2000), of(30, 40, 50))
    )
  }

  mergeMapforUser() {
    return of(1, 2, 3)
      .pipe(
        // mergeMap(ids => from(ids)),                 // emit each id     convert array → stream of ids
        tap(id => console.log('ID:', id)),          // log each id
        mergeMap(id => this.http.get(`https://jsonplaceholder.typicode.com/users/${id}`))     // fetch details in parallel
      )
  }
}


